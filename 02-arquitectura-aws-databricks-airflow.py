# Databricks notebook source
# 02-arquitectura-aws-databricks-airflow.py — Modulo 2 (75 min)

# COMMAND ----------

# MAGIC %run ./_resource/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC # Modulo 2 — Arquitectura AWS + Databricks + Airflow
# MAGIC **75 minutos** &nbsp;|&nbsp; S3 como lago Medallion · Databricks como motor · Airflow como orquestador · Pipeline real en vivo
# MAGIC
# MAGIC En este modulo ejecutamos el pipeline completo de Delfos: desde la ingesta de JSON crudo
# MAGIC en S3 hasta las alertas de riesgo en capa Gold, pasando por validacion de calidad en Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquitectura de referencia de Delfos
# MAGIC
# MAGIC El dato en Delfos recorre tres capas antes de llegar al consumidor:
# MAGIC
# MAGIC ```
# MAGIC [Fuentes de datos]
# MAGIC   App Nequi | Corresponsal | QR Pagos | API Terceros
# MAGIC           |
# MAGIC           v  JSON por archivo diario (generate_data.py -> aws s3 sync)
# MAGIC
# MAGIC [AWS S3 — Data Lake con Arquitectura Medallion]
# MAGIC   Bronze : dato crudo, JSON original, inmutable, retencion 5 anos (SFC)
# MAGIC   Silver : dato limpio, Delta Lake, schema validado, fuente de verdad
# MAGIC   Gold   : reglas de negocio aplicadas, listo para consumo analitico
# MAGIC           |
# MAGIC           v  Databricks Structured Streaming + foreachBatch
# MAGIC
# MAGIC [Consumidores]
# MAGIC   BI / Dashboards | Modelos ML | Alertas de Riesgo | Reportes SFC
# MAGIC ```
# MAGIC
# MAGIC **Databricks** es el motor de procesamiento: Spark 3.5, Auto Loader, Unity Catalog, MLflow.
# MAGIC **Airflow** es el orquestador: decide cuando corre cada job, en que orden y con que reintentos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisiones de arquitectura — por que esta combinacion
# MAGIC
# MAGIC | Decision | Alternativa descartada | Razon |
# MAGIC |---|---|---|
# MAGIC | **S3 + Delta Lake** como lago | Redshift Spectrum | Delta Time Travel permite auditoria regulatoria: reconstruir estado historico para la SFC |
# MAGIC | **Auto Loader** para ingesta | COPY INTO / S3 Event Notifications | Escala a millones de archivos sin gestionar listas de archivos ya procesados |
# MAGIC | **Airflow en EC2** como orquestador | MWAA (Managed Airflow) | Free Tier en t2.micro para el taller; en produccion se migraria a MWAA |
# MAGIC | **Unity Catalog** para gobernanza | AWS Glue Data Catalog | RLS nativa, Column Masks, Lineage y Audit Logs integrados — exigencia SARLAFT |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.0 — Estado del entorno antes de iniciar
# MAGIC
# MAGIC Si ejecutaste el Modulo 01 antes de este, la tabla `pagos.transacciones` ya tiene
# MAGIC datos sinteticos de Silver. El pipeline de este modulo va a APPEND datos adicionales
# MAGIC desde Auto Loader. Eso es correcto y muestra como el pipeline real acumula datos.
# MAGIC Si quieres un entorno limpio, usa el widget **reset = Si** en el setup y vuelve a ejecutar.

# COMMAND ----------

# DBTITLE 1,2.0 — Verificar estado del entorno
if spark.catalog.tableExists(f"{CATALOG}.{SCH_PAGOS}.transacciones"):
    n_prev = spark.table(f"{CATALOG}.{SCH_PAGOS}.transacciones").filter("capa='silver'").count()
    if n_prev > 0:
        print(f"[INFO] La tabla {CATALOG}.{SCH_PAGOS}.transacciones tiene {n_prev:,} registros Silver")
        print(f"       (del Modulo 01 u ejecuciones anteriores)")
        print(f"       El pipeline de este modulo agregara datos de Auto Loader sobre los existentes.")
        print(f"       Para reiniciar desde cero: widget 'reset = Si — reiniciar datos' y re-ejecutar setup.")
    else:
        print(f"[OK] Tabla existente sin registros Silver — lista para el pipeline.")
else:
    print(f"[OK] Primera ejecucion — el pipeline creara las tablas automaticamente.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 — Verificar datos en S3 antes de iniciar el pipeline
# MAGIC
# MAGIC Antes de arrancar el pipeline verificamos que los archivos JSON generados por
# MAGIC `generate_data.py` estan disponibles en el bucket S3. Si no hay archivos, el
# MAGIC Auto Loader no tiene nada que leer y el stream termina inmediatamente sin error.
# MAGIC
# MAGIC El script `infraestructura/generate_data.py` genera los JSON localmente.
# MAGIC El comando `aws s3 sync ./bronze/transacciones/ s3://BUCKET/bronze/transacciones/`
# MAGIC los sube al bucket antes de ejecutar este notebook.

# COMMAND ----------

# DBTITLE 1,2.1 — Verificar archivos disponibles en S3
# Listar los archivos JSON en la ruta Bronze del bucket S3
try:
    archivos = dbutils.fs.ls(f"{PATH_BRONZE}transacciones/")
    print(f"Archivos encontrados en {PATH_BRONZE}transacciones/:")
    print(f"  {'Archivo':<40} {'Tamano (KB)':>12}")
    print(f"  {'-'*40} {'-'*12}")
    for f in sorted(archivos, key=lambda x: x.name):
        print(f"  {f.name:<40} {f.size/1024:>12.1f}")
    print(f"\n  Total: {len(archivos)} archivo(s), {sum(f.size for f in archivos)/1024:.1f} KB")
except Exception as e:
    print(f"[ATENCION] No se encontraron archivos en S3: {e}")
    print()
    print("Para generar y subir datos al bucket, ejecutar en la terminal local:")
    print(f"  cd infraestructura")
    print(f"  python3 generate_data.py --tx 15000 --days 7 --seed 42")
    print(f"  aws s3 sync ./bronze/transacciones/ s3://{S3_BUCKET}/bronze/transacciones/")
    print()
    print("Puedes continuar con las celdas siguientes despues de subir los archivos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 — Paso 1: Ingesta Bronze con Auto Loader
# MAGIC
# MAGIC `spark.read.json()` escanea **todos** los archivos del bucket en cada ejecucion.
# MAGIC Con millones de transacciones diarias eso es costoso e ineficiente.
# MAGIC
# MAGIC **Auto Loader** (`cloudFiles`) mantiene un checkpoint interno de los archivos ya procesados
# MAGIC y lee unicamente los nuevos, sin importar cuantos se hayan acumulado en el bucket.
# MAGIC
# MAGIC Dos columnas de auditoria se agregan en Bronze:
# MAGIC - `_archivo_origen`: ruta exacta del JSON en S3 — permite rastrear el origen de cada registro
# MAGIC - `_ingested_at`: timestamp de cuando Delfos proceso ese archivo

# COMMAND ----------

# DBTITLE 1,2.2 — Ingesta Bronze: Auto Loader sobre S3
df_bronze = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "json")
    .option("cloudFiles.schemaLocation",      f"{PATH_CKPT}schema/")
    .option("cloudFiles.inferColumnTypes",   "true")
    .option("cloudFiles.maxFilesPerTrigger", "500")
    .load(f"{PATH_BRONZE}transacciones/")
    .withColumn("_archivo_origen", F.col("_metadata.file_path"))
    .withColumn("_ingested_at",    F.current_timestamp())
    .withColumn("capa",            F.lit("bronze"))
    .withColumn("es_fraude_real",  F.lit(False))   # campo de label — desconocido en raw
)

print("Schema inferido por Auto Loader desde los JSON de S3:")
display(spark.createDataFrame(
    [(f.name, f.dataType.simpleString(), f.nullable) for f in df_bronze.schema.fields],
    ["columna", "tipo", "nullable"]
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 — Como se ve el dato en Bronze antes de limpiar
# MAGIC
# MAGIC Bronze es el dato crudo tal como llega de la fuente: puede tener montos negativos,
# MAGIC canales invalidos, duplicados o timestamps futuros. Su unico compromiso es
# MAGIC **preservar el dato original exactamente como llego** — nunca modificarlo.
# MAGIC
# MAGIC La siguiente celda guarda un snapshot de Bronze en una tabla auxiliar de muestra
# MAGIC para poder compararlo con Silver despues del pipeline.

# COMMAND ----------

# DBTITLE 1,2.3 — Snapshot de Bronze para comparar con Silver
# .toTable() puede fallar en Unity Catalog si el schema de la tabla no existe aun.
# La alternativa robusta es escribir a una ruta Delta en S3 y registrar la tabla
# explicitamente con CREATE TABLE IF NOT EXISTS LOCATION.
_snapshot_path = f"{PATH_BRONZE}_snapshots/bronze_snapshot"

q_snapshot = (df_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", f"{PATH_CKPT}bronze_snapshot/")
    .option("path", _snapshot_path)
    .trigger(availableNow=True)   # availableNow: reemplaza al deprecado trigger(once=True) en DBR 10.4+
    .start())
q_snapshot.awaitTermination()

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCH_PAGOS}.bronze_snapshot
    USING DELTA LOCATION '{_snapshot_path}'
""")

print("Muestra de datos Bronze — exactamente como llegan de S3:")
spark.table(f"{CATALOG}.{SCH_PAGOS}.bronze_snapshot").show(10, truncate=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 — Paso 2: Bronze a Silver (validacion, limpieza e idempotencia)
# MAGIC
# MAGIC En este pipeline hay **dos niveles de deduplicacion** con propositos distintos:
# MAGIC
# MAGIC | Nivel | Mecanismo | Que protege |
# MAGIC |---|---|---|
# MAGIC | **Dentro del batch** | `dropDuplicates(["transaction_id"])` en `aplicar_reglas_calidad` | Mismo `transaction_id` aparece dos veces en el mismo archivo JSON |
# MAGIC | **Entre batches** | `MERGE INTO` en vez de `mode("append")` | Mismo archivo reprocesado por fallo de checkpoint, o mismo `transaction_id` en dos archivos distintos |
# MAGIC
# MAGIC `mode("append")` no es idempotente: si el stream falla y se reinicia, los archivos
# MAGIC ya procesados pueden escribirse dos veces. `MERGE INTO` compara por `transaction_id`
# MAGIC antes de insertar — si ya existe, lo ignora. Este es el patron oficial de Databricks
# MAGIC para garantizar **exactamente una escritura** (exactly-once semantics) en Delta Lake.
# MAGIC
# MAGIC `aplicar_reglas_calidad` (building block del Modulo 3) ademas rechaza:
# MAGIC - Montos negativos o cero
# MAGIC - Canales fuera de `app | qr | corresponsal | api`
# MAGIC - Timestamps no convertibles a TIMESTAMP

# COMMAND ----------

# DBTITLE 1,2.4 — Transformacion Bronze a Silver con foreachBatch + MERGE INTO
def bronze_a_silver(batch_df, batch_id):
    """Valida, limpia y escribe cada micro-batch en Silver. MERGE INTO garantiza idempotencia."""
    df_limpio = (aplicar_reglas_calidad(batch_df)
        .withColumn("capa", F.lit("silver")))

    n_raw      = batch_df.count()
    n_limpio   = df_limpio.count()
    rechazados = n_raw - n_limpio

    if n_limpio == 0:
        print(f"  Batch {batch_id:03d}: sin registros validos despues de calidad.")
        return

    # MERGE INTO en vez de mode("append") — garantiza exactamente una escritura por transaction_id.
    # Si el stream se reinicia y reprocesa el mismo archivo, los registros ya existentes
    # se ignoran (WHEN NOT MATCHED = solo inserta si no existe).
    _vista = f"_batch_silver_{batch_id}"
    df_limpio.createOrReplaceTempView(_vista)
    spark.sql(f"""
        MERGE INTO {CATALOG}.{SCH_PAGOS}.transacciones t
        USING {_vista} s
        ON  t.transaction_id = s.transaction_id
        AND t.capa           = 'silver'
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"  Batch {batch_id:03d}: {n_raw:>5,} raw -> {n_limpio:>5,} validos"
          f"  ({rechazados:>4,} rechazados calidad) — escritura via MERGE INTO")

q_silver = (df_bronze.writeStream
    .foreachBatch(bronze_a_silver)
    .option("checkpointLocation", f"{PATH_CKPT}bronze_to_silver/")
    .trigger(availableNow=True)   # availableNow: reemplaza trigger(once=True), DBR 10.4+
    # En produccion: .trigger(processingTime='5 minutes') para stream continuo
    .start())
q_silver.awaitTermination()

n_silver = spark.table(f"{CATALOG}.{SCH_PAGOS}.transacciones").filter("capa='silver'").count()
print(f"\nBronze -> Silver completado: {n_silver:,} registros en {CATALOG}.{SCH_PAGOS}.transacciones")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 — Comparar Bronze vs Silver: cuanto limpia el pipeline
# MAGIC
# MAGIC Esta comparacion muestra cuantos registros fueron rechazados por las reglas de calidad
# MAGIC y por que razon. Es el control de calidad del pipeline: si el porcentaje de rechazo
# MAGIC es inusualmente alto, hay un problema en la fuente de datos.

# COMMAND ----------

# DBTITLE 1,2.5a — Comparacion Bronze vs Silver por calidad
n_bronze = spark.table(f"{CATALOG}.{SCH_PAGOS}.bronze_snapshot").count()
n_silver = spark.table(f"{CATALOG}.{SCH_PAGOS}.transacciones").filter("capa='silver'").count()
n_rechazados = n_bronze - n_silver

print("=" * 55)
print("  Reporte de calidad: Bronze vs Silver")
print("=" * 55)
print(f"  Registros Bronze (crudo)  : {n_bronze:>10,}")
print(f"  Registros Silver (limpio) : {n_silver:>10,}")
print(f"  Rechazados por calidad    : {n_rechazados:>10,}  ({100*n_rechazados/max(n_bronze,1):.1f}%)")
print("=" * 55)

# COMMAND ----------

# DBTITLE 1,2.5b — Distribucion de canales en Silver (validacion de calidad)
# Canales invalidos deben ser 0: todos los registros con canal fuera de la lista
# fueron rechazados por aplicar_reglas_calidad() antes de escribir en Silver
spark.sql(f"""
SELECT
    canal,
    COUNT(*)                                       AS total,
    ROUND(AVG(monto), 0)                           AS monto_promedio_cop,
    ROUND(MIN(monto), 0)                           AS monto_minimo,
    ROUND(MAX(monto), 0)                           AS monto_maximo,
    SUM(CASE WHEN monto <= 0 THEN 1 ELSE 0 END)    AS montos_invalidos
FROM {CATALOG}.{SCH_PAGOS}.transacciones
WHERE capa = 'silver'
GROUP BY canal
ORDER BY total DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 — Delta DESCRIBE HISTORY: trazabilidad de cada escritura
# MAGIC
# MAGIC Delta Lake registra automaticamente cada operacion de escritura sobre la tabla.
# MAGIC Esto permite auditar: quien escribio, cuantos registros, desde que archivo y cuando.
# MAGIC A diferencia de un log de aplicacion, este registro es inmutable y vive con la tabla.

# COMMAND ----------

# DBTITLE 1,2.6 — Historial de escrituras en pagos.transacciones
# Cada version de la tabla corresponde a una operacion de escritura del pipeline.
# 'operationMetrics' muestra cuantos archivos y bytes se escribieron en cada version.
spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCH_PAGOS}.transacciones").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.7 — Paso 3: Silver a Gold (reglas de negocio del dominio riesgo)
# MAGIC
# MAGIC En Gold se aplican las reglas de negocio del dominio. Para riesgo, Delfos implementa
# MAGIC dos algoritmos de deteccion complementarios que actuan como building blocks:
# MAGIC
# MAGIC - **Z-score por usuario** (`umbral=3.0`): monto mas de 3 desviaciones estandar por encima
# MAGIC   del historial propio del usuario. Captura montos inusualmente altos para ese usuario.
# MAGIC - **Frecuencia en ventana** (`600 seg, max_tx=5`): mas de 5 transacciones en 10 minutos
# MAGIC   para el mismo usuario. Detecta rafagas tipicas de fraude por cuenta comprometida.
# MAGIC
# MAGIC Un registro puede activar ninguna, una o ambas reglas. El campo `nivel_riesgo` combina
# MAGIC ambas senales en una clasificacion de tres niveles: NORMAL, MEDIO, CRITICO.

# COMMAND ----------

# DBTITLE 1,2.7 — Silver a Gold: aplicar reglas de deteccion de anomalias
df_silver = spark.table(f"{CATALOG}.{SCH_PAGOS}.transacciones").filter("capa='silver'")

df_gold = (df_silver
    .withColumn("ts", F.to_timestamp(F.col("ts")))
    .transform(lambda d: zscore_por_usuario(d, umbral=3.0))
    .transform(lambda d: frecuencia_ventana(d, ventana_seg=600, max_tx=5))
    .withColumn("alerta",
        F.col("alerta_zscore") | F.col("alerta_frecuencia"))
    .withColumn("score",
        (F.col("alerta_zscore").cast("int") + F.col("alerta_frecuencia").cast("int")) / 2.0)
    .withColumn("nivel_riesgo",
        F.when(F.col("score") == 1.0, "CRITICO")
         .when(F.col("score") >  0.0, "MEDIO")
         .otherwise("NORMAL"))
    .withColumn("capa", F.lit("gold"))
    .withColumn("_procesado_en", F.current_timestamp())
)

# Etiquetado retrospectivo de fraude confirmado.
# Los JSON de S3 no traen label de fraude porque en produccion ese label lo genera
# el equipo de investigacion despues de revisar cada caso. Para el demo simulamos
# ese proceso: si AMBAS reglas (z-score Y frecuencia) se activaron para el mismo
# usuario, lo marcamos como fraude confirmado — doble evidencia independiente.
_fraud_ids = [
    r.user_id for r in (
        df_gold
        .filter(F.col("alerta_zscore") & F.col("alerta_frecuencia"))
        .select("user_id").distinct()
        .collect()
    )
]
df_gold = df_gold.withColumn(
    "es_fraude_real",
    F.col("user_id").isin(_fraud_ids) if _fraud_ids else F.lit(False)
)
print(f"Usuarios marcados como fraude confirmado (doble alerta): {len(_fraud_ids)}")

(df_gold.write.format("delta").mode("overwrite")
    .option("overwriteSchema","true")
    .partitionBy("capa")
    .saveAsTable(f"{CATALOG}.{SCH_RIESGO}.alertas"))

# Leer de la tabla guardada — evita re-ejecutar toda la cadena de transformaciones
_df_alertas = spark.table(f"{CATALOG}.{SCH_RIESGO}.alertas")
n_alertas   = _df_alertas.filter("alerta").count()
print(f"Silver -> Gold: {_df_alertas.count():,} registros, {n_alertas:,} alertas generadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.8 — Analizar los resultados Gold: precision y recall
# MAGIC
# MAGIC En produccion, el label `es_fraude_real` lo genera el equipo de investigacion despues
# MAGIC de revisar cada alerta manualmente. En este demo simulamos ese proceso con **etiquetado
# MAGIC retrospectivo**: marcamos como fraude confirmado a los usuarios donde AMBAS reglas
# MAGIC (z-score Y frecuencia) se activaron — doble evidencia independiente = alta confianza.
# MAGIC
# MAGIC Esto permite calcular precision, recall y F1 — las metricas que el equipo de riesgo
# MAGIC revisaria cada semana para decidir si ajustar umbrales del detector.

# COMMAND ----------

# DBTITLE 1,2.8a — Metricas de evaluacion del motor de deteccion
# Leer de la tabla guardada para no re-ejecutar el pipeline de transformaciones
_df_m = spark.table(f"{CATALOG}.{SCH_RIESGO}.alertas").cache()

_t   = _df_m.count()
_a   = _df_m.filter("alerta").count()
_vp  = _df_m.filter("alerta AND es_fraude_real").count()
_fp  = _a - _vp
_fn  = _df_m.filter("NOT alerta AND es_fraude_real").count()
_pr  = round(_vp / _a * 100, 1)           if _a          else 0
_re  = round(_vp / (_vp + _fn) * 100, 1)  if (_vp + _fn) else 0
_f1  = round(2 * _pr * _re / (_pr + _re), 1) if (_pr + _re) else 0
_a_z = _df_m.filter("alerta_zscore").count()
_a_f = _df_m.filter("alerta_frecuencia").count()
_df_m.unpersist()

print("=" * 60)
print(f"  Motor de deteccion: {CATALOG}.{SCH_RIESGO}.alertas [gold]")
print("=" * 60)
print(f"  Transacciones total       : {_t:>10,}")
print(f"  Alertas generadas         : {_a:>10,}  ({round(_a/_t*100,1) if _t else 0}%)")
print("-" * 60)
print(f"  Verdaderos positivos (TP) : {_vp:>10,}  (fraude detectado correctamente)")
print(f"  Falsos positivos (FP)     : {_fp:>10,}  (alerta erronea)")
print(f"  Falsos negativos (FN)     : {_fn:>10,}  (fraude no detectado)")
print("-" * 60)
print(f"  Precision                 : {_pr:>10}%  (de cada alerta, cuantas son fraude real)")
print(f"  Recall                    : {_re:>10}%  (del fraude real, cuanto detecto)")
print(f"  F1-Score                  : {_f1:>10}%")
print("-" * 60)
print(f"  Regla 1 — Z-score >3o    : {_a_z:>10,} alertas")
print(f"  Regla 2 — Frecuencia     : {_a_f:>10,} alertas")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,2.8b — Distribucion de alertas por nivel de riesgo y canal
# El canal 'api' deberia concentrar la mayoria de alertas por frecuencia
# (es el canal mas facil de automatizar para un atacante)
spark.sql(f"""
SELECT
    nivel_riesgo,
    canal,
    COUNT(*)           AS total,
    SUM(CASE WHEN es_fraude_real THEN 1 ELSE 0 END) AS fraudes_reales,
    ROUND(AVG(monto))  AS monto_promedio
FROM {CATALOG}.{SCH_RIESGO}.alertas
WHERE alerta = true
GROUP BY nivel_riesgo, canal
ORDER BY nivel_riesgo DESC, total DESC
""").display()

# COMMAND ----------

# DBTITLE 1,2.8c — Usuarios con mas alertas en el periodo
# Top usuarios alertados: en produccion, estos usuarios serian escalados a la
# celula de investigacion de fraude para revision manual
spark.sql(f"""
SELECT
    user_id,
    COUNT(*)                                          AS total_alertas,
    SUM(CASE WHEN alerta_zscore     THEN 1 ELSE 0 END) AS alertas_zscore,
    SUM(CASE WHEN alerta_frecuencia THEN 1 ELSE 0 END) AS alertas_frecuencia,
    ROUND(MAX(monto), 0)                              AS monto_maximo_cop,
    MAX(nivel_riesgo)                                 AS max_nivel_riesgo,
    FIRST(ciudad)                                     AS ciudad
FROM {CATALOG}.{SCH_RIESGO}.alertas
WHERE alerta = true
GROUP BY user_id
ORDER BY total_alertas DESC
LIMIT 10
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.9 — El rol de Airflow: orquestar el pipeline completo
# MAGIC
# MAGIC Airflow no ejecuta Spark — eso lo hace Databricks. El rol de Airflow es la **orquestacion**:
# MAGIC decide cuando corre cada Job, en que orden, cuantos reintentos tiene y a quien notifica si
# MAGIC algo falla. Las tareas estan encadenadas con el operador `>>`:
# MAGIC
# MAGIC ```
# MAGIC generar_datos >> bronze_to_silver >> silver_to_gold
# MAGIC ```
# MAGIC
# MAGIC En Delfos, el DAG corre cada dia a las 6:00 AM (America/Bogota) con 2 reintentos automaticos.
# MAGIC Si `bronze_to_silver` falla, `silver_to_gold` no arranca y el SLA de frescura queda afectado.
# MAGIC El DAG esta en `setup/dags/delfos_pipeline.py` y corre desde `http://IP-EC2:8080`.

# COMMAND ----------

# DBTITLE 1,2.9 — Leer el DAG de Airflow y mostrar su estructura
# El archivo del DAG vive en el repositorio — lo mostramos directamente para
# discutir su estructura con el grupo sin necesidad de abrir la UI de Airflow
dag_path = "/Workspace/Shared/delfos-m1-fundamentos/setup/dags/delfos_pipeline.py"
try:
    with open(dag_path) as _f:
        print(_f.read(3000))
except Exception:
    print("El DAG vive en setup/dags/delfos_pipeline.py en el repositorio.")
    print("Estructura del pipeline en Airflow:")
    print()
    print("  [generar_datos]")
    print("       |")
    print("       v")
    print("  [bronze_to_silver]  <- DatabricksRunNowOperator, job_id=bronze_silver_job")
    print("       |")
    print("       v")
    print("  [silver_to_gold]   <- DatabricksRunNowOperator, job_id=silver_gold_job")
    print()
    print("  schedule_interval : '0 11 * * *'  (6AM Bogota = 11 UTC)")
    print("  retries           : 2")
    print("  retry_delay       : 10 minutos")
    print("  email_on_failure  : datos-plataforma@nequi.com.co")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Resumen del modulo
# MAGIC
# MAGIC | Paso | Que hiciste | Resultado |
# MAGIC |:---:|---|---|
# MAGIC | 1 | Verificar archivos en S3 | Confirmar que hay datos para procesar |
# MAGIC | 2 | Auto Loader Bronze | Stream incremental desde S3 con checkpoint |
# MAGIC | 3 | Snapshot Bronze | Ver el dato crudo antes de limpiar |
# MAGIC | 4 | Bronze → Silver (foreachBatch) | Deduplicacion y validacion de calidad |
# MAGIC | 5 | Comparar Bronze vs Silver | Cuantificar cuanto limpia el pipeline |
# MAGIC | 6 | DESCRIBE HISTORY | Trazabilidad inmutable de cada escritura |
# MAGIC | 7 | Silver → Gold | Aplicar reglas de negocio del dominio riesgo |
# MAGIC | 8 | Metricas del motor | Precision, Recall y F1 del detector de fraude |
# MAGIC | 9 | Analisis de alertas | Top usuarios, distribucion por canal y nivel |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Reflexion y discusion
# MAGIC
# MAGIC **Reflexion**
# MAGIC
# MAGIC Usamos `trigger(availableNow=True)` para el stream Bronze a Silver. Cuales serian las ventajas
# MAGIC y desventajas de cambiarlo a `trigger(processingTime='5 minutes')` en produccion?
# MAGIC Como afecta ese cambio al SLA de 1h del Data Product?
# MAGIC
# MAGIC **Discusion**
# MAGIC
# MAGIC Airflow vive en una EC2 t2.micro con 1GB de RAM. Si el DAG necesita orquestar 50 pipelines
# MAGIC distintos en Databricks, esa misma instancia escala? Compara MWAA vs. EC2 vs. ECS Fargate.
# MAGIC
# MAGIC **Referencia:** [Delta Lake — Medallion Architecture](https://docs.databricks.com/en/lakehouse/medallion.html)
