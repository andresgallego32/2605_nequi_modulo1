# Databricks notebook source
# 01-data-mesh-delfos.py — Modulo 1: Introduccion a Data Mesh aplicado a Delfos (45 min)

# COMMAND ----------

# MAGIC %run ./_resource/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC # Modulo 1 — Introduccion a Data Mesh aplicado a Delfos
# MAGIC **45 minutos** &nbsp;|&nbsp; Dominios de negocio · Data Products · Gobernanza federada · Plataforma self-serve
# MAGIC
# MAGIC En este modulo construimos el primer Data Product de Delfos desde cero: desde la estructura
# MAGIC del catalogo hasta el contrato de gobernanza, la seguridad a nivel de columna y la
# MAGIC trazabilidad regulatoria que exige la SFC.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Por que Data Mesh en Nequi?
# MAGIC
# MAGIC Nequi tiene un problema clasico de escala: el equipo central de datos se convirtio en un cuello
# MAGIC de botella. Pagos necesita un dashboard, riesgo necesita features para el modelo, producto
# MAGIC necesita una cohorte — los tres hacen fila con el mismo equipo de ingenieria de datos,
# MAGIC que no puede atender todo a la vez.
# MAGIC
# MAGIC **Data Mesh** (Zhamak Dehghani, 2019) propone un cambio de paradigma: en lugar de centralizar
# MAGIC el dato, **descentralizar la responsabilidad** de producirlo hacia los equipos que mejor lo
# MAGIC conocen. Delfos es la implementacion de esos principios en Nequi.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Los 4 principios de Data Mesh — implementacion en Delfos
# MAGIC
# MAGIC | Principio | Descripcion | Implementacion en Delfos |
# MAGIC |---|---|---|
# MAGIC | **1. Propiedad orientada al dominio** | Cada equipo es dueno de sus datos. Nadie mas puede publicar en su schema. | `nequi_prod.pagos.*` · `nequi_prod.riesgo.*` · `nequi_prod.clientes.*` |
# MAGIC | **2. Datos como producto** | El Data Product incluye propietario, SLA, schema documentado, tests de calidad y clasificacion de seguridad. | Unity Catalog + `TBLPROPERTIES` + Great Expectations |
# MAGIC | **3. Plataforma self-serve** | Los equipos crean pipelines y consultan datos sin depender del equipo central. | Building Blocks reutilizables + Asset Bundles + Unity Catalog autodescubrible |
# MAGIC | **4. Gobernanza federada** | Autonomia por dominio respetando estandares globales: SARLAFT, Circular 052 SFC, Habeas Data. | Unity Catalog RLS + Column Masks + Audit Logs en `system.access.audit` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 — Estructura de dominios en Unity Catalog
# MAGIC
# MAGIC El setup de Delfos crea cuatro schemas en el catalogo `nequi_prod`, uno por dominio de negocio.
# MAGIC Cada schema es propiedad exclusiva del equipo correspondiente.
# MAGIC Observa que la estructura es plana: `catalogo.dominio.tabla` — sin jerarquias adicionales.

# COMMAND ----------

# DBTITLE 1,1.1 — Dominios registrados en Unity Catalog
# MAGIC %sql
# MAGIC -- El catalogo nequi_prod organiza los datos por dominio de negocio.
# MAGIC -- Esta es la estructura de gobierno que habilita el Principio 1 de Data Mesh.
# MAGIC SHOW SCHEMAS IN nequi_prod;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 — Cargar datos de muestra para el modulo
# MAGIC
# MAGIC Para poder demostrar todos los conceptos de Data Mesh en este modulo sin depender del
# MAGIC pipeline completo de S3 (Modulo 2), generamos 2.500 transacciones sinteticas directamente
# MAGIC en Spark e inyectamos patrones de fraude reales para que las demostraciones de gobernanza
# MAGIC sean significativas.
# MAGIC
# MAGIC Los datos siguen el schema exacto de produccion — incluyendo distribucion realista de canales,
# MAGIC ciudades colombianas y patrones de monto log-normales (media ~85.000 COP, tipico de Nequi).

# COMMAND ----------

# DBTITLE 1,1.2 — Generacion de datos sinteticos para el modulo
import pandas as pd, uuid, random
from datetime import datetime, timedelta

random.seed(42)

CANALES  = ["app", "qr", "corresponsal", "api"]
CIUDADES = ["Bogota", "Medellin", "Cali", "Barranquilla", "Cartagena",
            "Bucaramanga", "Pereira", "Cucuta", "Ibague", "Santa Marta"]
N_USERS, N_TX = 80, 2_500
users = [f"u_{i:04d}" for i in range(1, N_USERS + 1)]
now   = datetime.now()

# Marcar 8% de usuarios como potencialmente fraudulentos
fraud_monto = set(random.sample(users, 6))
fraud_freq  = set(random.sample(users, 5))

rows = []
for _ in range(N_TX):
    uid  = random.choice(users)
    ts   = now - timedelta(hours=random.randint(0, 168))
    monto_base = round(max(1_000, min(random.lognormvariate(11.35, 1.1), 9_999_999)), 2)
    es_fraude  = False

    # Inyectar fraude por monto (z-score alto)
    if uid in fraud_monto and random.random() < 0.05:
        monto_base = round(random.uniform(4_500_000, 9_800_000), 2)
        es_fraude  = True

    rows.append({
        "transaction_id": str(uuid.uuid4()),
        "user_id"       : uid,
        "monto"         : monto_base,
        "canal"         : random.choices(CANALES, weights=[55, 25, 12, 8])[0],
        "ciudad"        : random.choices(CIUDADES, weights=[30,22,13,9,7,5,3,3,4,4])[0],
        "dispositivo"   : f"{'Android' if random.random()<0.72 else 'iOS'}-{uid[-4:].upper()}",
        "ts"            : ts.strftime("%Y-%m-%dT%H:%M:%S"),
        "capa"          : "silver",          # cargamos directamente como silver (ya validado)
        "es_fraude_real": es_fraude,
        "_procesado_en" : now,
    })

# Agregar rafaga de frecuencia para usuarios marcados (patron de fraude por API)
for uid in list(fraud_freq)[:3]:
    ts_burst = now - timedelta(hours=random.randint(1, 48))
    for i in range(8):                        # 8 tx en 4 minutos: supera max_tx=5
        rows.append({
            "transaction_id": str(uuid.uuid4()),
            "user_id"       : uid,
            "monto"         : round(random.uniform(10_000, 80_000), 2),
            "canal"         : "api",
            "ciudad"        : "Bogota",
            "dispositivo"   : f"Android-{uid[-4:].upper()}",
            "ts"            : (ts_burst + timedelta(seconds=i*30)).strftime("%Y-%m-%dT%H:%M:%S"),
            "capa"          : "silver",
            "es_fraude_real": True,
            "_procesado_en" : now,
        })

df_muestra = spark.createDataFrame(pd.DataFrame(rows))
(df_muestra.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("capa")
    .saveAsTable(f"{CATALOG}.pagos.transacciones"))

n_total  = df_muestra.count()
n_fraude = df_muestra.filter("es_fraude_real").count()
print(f"Datos cargados en {CATALOG}.pagos.transacciones")
print(f"  Total       : {n_total:,} transacciones")
print(f"  Fraudulentas: {n_fraude:,}  ({100*n_fraude/n_total:.1f}%)")
print(f"  Usuarios    : {N_USERS}")
print(f"  Periodo     : ultimas 7 dias")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 — Explorar el Data Product: perfil de los datos
# MAGIC
# MAGIC Antes de registrar el contrato formal del Data Product, perfilamos los datos para entender
# MAGIC su distribucion. Esto es lo que el equipo de pagos haria antes de publicar la primera version:
# MAGIC verificar que los datos son los esperados y que el schema es correcto.

# COMMAND ----------

# DBTITLE 1,1.3a — Distribucion por canal de pago
# MAGIC %sql
# MAGIC -- Distribucion de transacciones por canal: verificar que los pesos son realistas
# MAGIC -- La app debe ser el canal dominante (~55% en Nequi)
# MAGIC SELECT
# MAGIC     canal,
# MAGIC     COUNT(*)                                    AS total_transacciones,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS porcentaje,
# MAGIC     ROUND(AVG(monto), 0)                        AS monto_promedio_cop,
# MAGIC     ROUND(MIN(monto), 0)                        AS monto_minimo,
# MAGIC     ROUND(MAX(monto), 0)                        AS monto_maximo,
# MAGIC     ROUND(PERCENTILE(monto, 0.5), 0)            AS mediana_cop,
# MAGIC     ROUND(PERCENTILE(monto, 0.95), 0)           AS percentil_95
# MAGIC FROM nequi_prod.pagos.transacciones
# MAGIC WHERE capa = 'silver'
# MAGIC GROUP BY canal
# MAGIC ORDER BY total_transacciones DESC;

# COMMAND ----------

# DBTITLE 1,1.3b — Distribucion geografica de transacciones
# MAGIC %sql
# MAGIC -- Distribucion por ciudad: Bogota y Medellin concentran el grueso del volumen
# MAGIC SELECT
# MAGIC     ciudad,
# MAGIC     COUNT(*)                                           AS total,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS porcentaje,
# MAGIC     ROUND(AVG(monto), 0)                               AS monto_promedio_cop,
# MAGIC     SUM(monto) / 1e6                                   AS volumen_millones_cop
# MAGIC FROM nequi_prod.pagos.transacciones
# MAGIC WHERE capa = 'silver'
# MAGIC GROUP BY ciudad
# MAGIC ORDER BY total DESC;

# COMMAND ----------

# DBTITLE 1,1.3c — Actividad por hora del dia (patron de uso)
# MAGIC %sql
# MAGIC -- Patron de uso por hora: permite validar que los timestamps son coherentes con
# MAGIC -- el comportamiento esperado de usuarios (picos en manana y tarde)
# MAGIC SELECT
# MAGIC     HOUR(ts)          AS hora_utc,
# MAGIC     COUNT(*)          AS transacciones,
# MAGIC     ROUND(AVG(monto)) AS monto_promedio
# MAGIC FROM nequi_prod.pagos.transacciones
# MAGIC WHERE capa = 'silver'
# MAGIC GROUP BY hora_utc
# MAGIC ORDER BY hora_utc;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 — Registrar el contrato del Data Product
# MAGIC
# MAGIC Un **Data Product** en Delfos no es solo una tabla — es un contrato.
# MAGIC `TBLPROPERTIES` es el mecanismo de Unity Catalog para registrar ese contrato junto con el dato:
# MAGIC quien lo posee, con que SLA, bajo que normas regulatorias y en que estado del ciclo de vida.
# MAGIC
# MAGIC El contrato vive con el dato — no en un documento externo que nadie actualiza.
# MAGIC Cualquier equipo puede consultar el contrato con `DESCRIBE EXTENDED` sin buscar documentacion adicional.

# COMMAND ----------

# DBTITLE 1,1.4a — Registrar el contrato completo con TBLPROPERTIES
# MAGIC %sql
# MAGIC ALTER TABLE nequi_prod.pagos.transacciones
# MAGIC SET TBLPROPERTIES (
# MAGIC     -- Identidad del Data Product
# MAGIC     'delfos.product_id'       = 'pagos.transacciones.v1',
# MAGIC     'delfos.domain'           = 'pagos',
# MAGIC     'delfos.owner_team'       = 'equipo-pagos',
# MAGIC     'delfos.owner_email'      = 'datos-pagos@nequi.com.co',
# MAGIC     -- Contrato de calidad
# MAGIC     'delfos.sla_freshness'    = '1h',
# MAGIC     'delfos.sla_availability' = '99.5%',
# MAGIC     'delfos.quality_tests'    = 'great_expectations::pagos_suite_v1',
# MAGIC     -- Clasificacion de seguridad
# MAGIC     'delfos.classification'   = 'CONFIDENCIAL',
# MAGIC     'delfos.pii_columns'      = 'user_id,dispositivo',
# MAGIC     -- Regulatorio
# MAGIC     'delfos.regulatory'       = 'SARLAFT,Circular-052-SFC,Habeas-Data-1581',
# MAGIC     'delfos.retention_days'   = '1825',   -- 5 anos (requerimiento SFC)
# MAGIC     -- Ciclo de vida
# MAGIC     'delfos.version'          = '1.0.0',
# MAGIC     'delfos.status'           = 'PRODUCCION'
# MAGIC );
# MAGIC
# MAGIC COMMENT ON TABLE nequi_prod.pagos.transacciones IS
# MAGIC     'Data Product: transacciones Nequi normalizadas y validadas. Fuente de verdad del dominio pagos. SLA: 1h. SARLAFT.';

# COMMAND ----------

# DBTITLE 1,1.4b — Verificar el contrato registrado
# MAGIC %sql
# MAGIC -- DESCRIBE EXTENDED muestra TBLPROPERTIES junto con estadisticas fisicas de la tabla.
# MAGIC -- Un analista de cumplimiento puede ejecutar esta consulta para auditar el contrato
# MAGIC -- sin necesitar acceso al codigo del pipeline.
# MAGIC DESCRIBE EXTENDED nequi_prod.pagos.transacciones;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 — Documentar columnas del Data Product
# MAGIC
# MAGIC El contrato de la tabla no es suficiente. Cada columna debe estar documentada con:
# MAGIC su significado de negocio, unidad, valores validos y si contiene PII.
# MAGIC
# MAGIC Unity Catalog almacena estos comentarios y los expone en el catalogo de datos,
# MAGIC en los resultados de `DESCRIBE TABLE` y en la UI de Databricks — no en una wiki externa
# MAGIC que siempre esta desactualizada.

# COMMAND ----------

# DBTITLE 1,1.5 — Documentar columnas con COMMENT ON COLUMN
# MAGIC %sql
# MAGIC ALTER TABLE nequi_prod.pagos.transacciones
# MAGIC     ALTER COLUMN transaction_id COMMENT 'UUID unico de la transaccion. Clave primaria del Data Product.';
# MAGIC
# MAGIC ALTER TABLE nequi_prod.pagos.transacciones
# MAGIC     ALTER COLUMN user_id COMMENT 'Identificador del usuario Nequi. PII — enmascarar en ambientes no-prod.';
# MAGIC
# MAGIC ALTER TABLE nequi_prod.pagos.transacciones
# MAGIC     ALTER COLUMN monto COMMENT 'Monto de la transaccion en pesos colombianos (COP). Siempre positivo.';
# MAGIC
# MAGIC ALTER TABLE nequi_prod.pagos.transacciones
# MAGIC     ALTER COLUMN canal COMMENT 'Canal de origen: app | qr | corresponsal | api. Validado en ingesta.';
# MAGIC
# MAGIC ALTER TABLE nequi_prod.pagos.transacciones
# MAGIC     ALTER COLUMN ciudad COMMENT 'Ciudad de origen de la transaccion segun geolocalización del dispositivo.';
# MAGIC
# MAGIC ALTER TABLE nequi_prod.pagos.transacciones
# MAGIC     ALTER COLUMN dispositivo COMMENT 'Hash del dispositivo movil. PII — enmascarar en ambientes no-prod.';
# MAGIC
# MAGIC ALTER TABLE nequi_prod.pagos.transacciones
# MAGIC     ALTER COLUMN ts COMMENT 'Timestamp de la transaccion en UTC (ISO 8601). Nunca en el futuro.';
# MAGIC
# MAGIC ALTER TABLE nequi_prod.pagos.transacciones
# MAGIC     ALTER COLUMN capa COMMENT 'Capa Medallion donde reside el registro: bronze | silver | gold.';
# MAGIC
# MAGIC -- Verificar que los comentarios quedaron registrados
# MAGIC DESCRIBE TABLE nequi_prod.pagos.transacciones;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 — DESCRIBE HISTORY: el log de transacciones de Delta
# MAGIC
# MAGIC Delta Lake mantiene un **log de transacciones** inmutable que registra cada operacion
# MAGIC sobre la tabla: quien escribio, cuantos registros, cuando y que operacion fue.
# MAGIC
# MAGIC Este log es el fundamento de la auditoria regulatoria en Delfos: la SFC puede solicitar
# MAGIC "muestra el estado de los datos de pagos el 15 de marzo a las 10 AM" y Delfos puede
# MAGIC responder en segundos con Delta Time Travel — sin necesidad de un sistema separado de auditoria.

# COMMAND ----------

# DBTITLE 1,1.6 — Log de transacciones Delta (historial completo de la tabla)
# MAGIC %sql
# MAGIC -- Cada fila es una version de la tabla.
# MAGIC -- 'operation' muestra que tipo de escritura fue: WRITE, MERGE, DELETE, ALTER TABLE...
# MAGIC -- 'operationParameters' tiene los detalles (modo de escritura, particion afectada, etc.)
# MAGIC DESCRIBE HISTORY nequi_prod.pagos.transacciones;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 — Gobernanza federada: Row-Level Security y Column Mask
# MAGIC
# MAGIC El **Principio 4** (gobernanza federada) en accion: el dominio de riesgo define quien puede
# MAGIC ver que dentro de sus propios datos, respetando los estandares globales de Nequi.
# MAGIC
# MAGIC La vista `v_transacciones` aplica dos controles simultaneos:
# MAGIC - **Row-Level Security**: el equipo de riesgo solo ve registros propios de su dominio.
# MAGIC - **Column Mask**: la columna `dispositivo` (PII) se enmascara para roles sin privilegio.
# MAGIC
# MAGIC Ambos controles son **transparentes para el consumidor**: consulta la vista como cualquier
# MAGIC tabla SQL y Unity Catalog aplica los filtros automaticamente segun el usuario activo.

# COMMAND ----------

# DBTITLE 1,1.7a — Crear vista con RLS y Column Mask
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW nequi_prod.riesgo.v_transacciones
# MAGIC COMMENT 'Vista del dominio riesgo sobre transacciones de pagos. RLS + Column Mask activos.'
# MAGIC AS
# MAGIC SELECT
# MAGIC     transaction_id,
# MAGIC     user_id,
# MAGIC     monto,
# MAGIC     canal,
# MAGIC     ciudad,
# MAGIC     ts,
# MAGIC     -- Column Mask: dispositivo (PII) visible solo para cumplimiento y admins
# MAGIC     CASE WHEN is_member('equipo-cumplimiento') OR is_member('admins')
# MAGIC          THEN dispositivo ELSE '***' END AS dispositivo,
# MAGIC     capa,
# MAGIC     es_fraude_real,
# MAGIC     _procesado_en
# MAGIC FROM nequi_prod.pagos.transacciones
# MAGIC WHERE capa = 'silver'
# MAGIC    OR is_member('admins')
# MAGIC    OR is_member('equipo-cumplimiento');

# COMMAND ----------

# DBTITLE 1,1.7b — Verificar que el enmascaramiento funciona para el usuario actual
# MAGIC %sql
# MAGIC -- El usuario del taller NO pertenece a equipo-cumplimiento ni admins,
# MAGIC -- por lo tanto debe ver '***' en la columna dispositivo.
# MAGIC SELECT
# MAGIC     current_user()  AS yo,
# MAGIC     COUNT(*)        AS registros_visibles,
# MAGIC     COUNT(DISTINCT dispositivo) AS dispositivos_distintos,  -- deberia ser 1 si todo es '***'
# MAGIC     FIRST(dispositivo)          AS muestra_dispositivo      -- debe mostrar '***'
# MAGIC FROM nequi_prod.riesgo.v_transacciones;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.8 — Delta Time Travel: auditoria regulatoria
# MAGIC
# MAGIC Una de las capacidades mas importantes de Delta Lake para el cumplimiento regulatorio.
# MAGIC La SFC puede pedir: *"muestra el estado de los datos de pagos al momento de la auditoria"*.
# MAGIC Con Delta Time Travel, Delfos puede reconstruir el estado exacto de cualquier tabla
# MAGIC en cualquier version o timestamp historico — sin sistemas de auditoria externos.
# MAGIC
# MAGIC Para que el demo sea significativo primero simulamos una segunda escritura
# MAGIC (como si el pipeline hubiera corrido una segunda vez con nuevas transacciones).

# COMMAND ----------

# DBTITLE 1,1.8a — Simular una segunda carga para crear la version 1 de la tabla
import pandas as pd, uuid, random
from datetime import datetime, timedelta

random.seed(99)
now = datetime.now()

# Generar 200 transacciones adicionales — simula el pipeline del dia siguiente
filas_v2 = []
for _ in range(200):
    uid = f"u_{random.randint(1, 80):04d}"
    filas_v2.append({
        "transaction_id": str(uuid.uuid4()),
        "user_id"       : uid,
        "monto"         : round(max(1_000, random.lognormvariate(11.5, 0.9)), 2),
        "canal"         : random.choices(["app","qr","corresponsal","api"], weights=[55,25,12,8])[0],
        "ciudad"        : random.choice(["Bogota","Medellin","Cali","Barranquilla"]),
        "dispositivo"   : f"Android-{uid[-4:].upper()}",
        "ts"            : (now - timedelta(hours=random.randint(0, 24))).strftime("%Y-%m-%dT%H:%M:%S"),
        "capa"          : "silver",
        "es_fraude_real": False,
        "_procesado_en" : now,
    })

df_v2 = spark.createDataFrame(pd.DataFrame(filas_v2))
(df_v2.write.format("delta").mode("append")
    .partitionBy("capa")
    .saveAsTable(f"{CATALOG}.pagos.transacciones"))

print(f"Version 1 creada: 200 registros adicionales agregados con mode='append'")
print(f"La tabla ahora tiene 2 versiones en el log de Delta.")

# COMMAND ----------

# DBTITLE 1,1.8b — DESCRIBE HISTORY: ver las dos versiones disponibles
# MAGIC %sql
# MAGIC -- Ahora hay dos versiones: la carga inicial (version 0) y la segunda carga (version 1).
# MAGIC -- Cada fila del log es una operacion de escritura — inmutable e irrepetible.
# MAGIC DESCRIBE HISTORY nequi_prod.pagos.transacciones;

# COMMAND ----------

# DBTITLE 1,1.8c — Time Travel: comparar version 0 vs version actual
# MAGIC %sql
# MAGIC -- La version 0 tiene 2.500 registros (carga inicial del modulo).
# MAGIC -- La version actual tiene 2.700 registros (2.500 + 200 de la segunda carga).
# MAGIC -- Esto es lo que un auditor de la SFC consulta para reconstruir el estado historico.
# MAGIC SELECT 'Version 0 (carga inicial)'  AS momento, COUNT(*) AS total, ROUND(AVG(monto)) AS monto_prom
# MAGIC FROM nequi_prod.pagos.transacciones VERSION AS OF 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Version 1 (carga siguiente)' AS momento, COUNT(*) AS total, ROUND(AVG(monto)) AS monto_prom
# MAGIC FROM nequi_prod.pagos.transacciones VERSION AS OF 1
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Version actual'              AS momento, COUNT(*) AS total, ROUND(AVG(monto)) AS monto_prom
# MAGIC FROM nequi_prod.pagos.transacciones;

# COMMAND ----------

# DBTITLE 1,1.8d — Time Travel: auditar una transaccion especifica en la version 0
# MAGIC %sql
# MAGIC -- Escenario real de auditoria SFC: verificar el monto original de una transaccion
# MAGIC -- antes de cualquier transformacion posterior. VERSION AS OF 0 garantiza
# MAGIC -- que vemos el dato tal como fue cargado inicialmente.
# MAGIC SELECT
# MAGIC     transaction_id,
# MAGIC     user_id,
# MAGIC     monto,
# MAGIC     canal,
# MAGIC     ciudad,
# MAGIC     ts,
# MAGIC     _procesado_en
# MAGIC FROM nequi_prod.pagos.transacciones VERSION AS OF 0
# MAGIC ORDER BY _procesado_en ASC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.9 — Descubrimiento self-serve: catalogo de Data Products
# MAGIC
# MAGIC **Principio 3** (plataforma self-serve): un analista del equipo de clientes puede descubrir
# MAGIC que Data Products existen en Delfos sin necesitar al equipo de ingenieria.
# MAGIC
# MAGIC La tabla `system.information_schema.tables` expone todos los objetos de Unity Catalog con
# MAGIC sus metadatos. Esta es la primera consulta que corre cualquier equipo nuevo que quiere
# MAGIC saber que datos tiene disponibles para consumir.

# COMMAND ----------

# DBTITLE 1,1.9a — Catalogo de Data Products en nequi_prod
# MAGIC %sql
# MAGIC     
# MAGIC SELECT
# MAGIC     table_schema   AS dominio,
# MAGIC     table_name     AS data_product,
# MAGIC     table_type,
# MAGIC     comment        AS descripcion,
# MAGIC     created        AS creado_en,
# MAGIC     last_altered   AS ultima_modificacion
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = 'nequi_prod'
# MAGIC   AND table_schema  != 'information_schema'
# MAGIC ORDER BY dominio, data_product;

# COMMAND ----------

# DBTITLE 1,1.9b — Consultar el contrato de un Data Product especifico
# MAGIC %sql
# MAGIC -- SHOW TBLPROPERTIES expone el contrato completo del Data Product.
# MAGIC -- Un consumidor puede ejecutar esta consulta sin buscar documentacion en otra herramienta.
# MAGIC SHOW TBLPROPERTIES nequi_prod.pagos.transacciones;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.10 — Auditoria de acceso: system.access.audit
# MAGIC
# MAGIC Unity Catalog registra automaticamente cada acceso a los Data Products en `system.access.audit`.
# MAGIC Esta tabla es exactamente lo que la SFC solicita en una auditoria SARLAFT: quien accedio
# MAGIC a que dato y cuando — sin instrumentacion adicional de parte del equipo de datos.
# MAGIC
# MAGIC > **Nota:** `system.access.audit` requiere Unity Catalog Enterprise o Premium con
# MAGIC > System Tables habilitadas. En cuentas trial puede no estar disponible.
# MAGIC > Para habilitarlo: Account Console > Unity Catalog > Metastore > System Tables.

# COMMAND ----------

# DBTITLE 1,1.10 — Registro de accesos a Data Products de Delfos
# En Unity Catalog, request_params es MAP<STRING,STRING> — acceso con corchetes, no punto.
# Envolvemos en try/except porque system.access.audit requiere permisos especiales.
try:
    df_audit = spark.sql(f"""
        SELECT
            DATE_TRUNC('minute', event_time)   AS minuto,
            user_identity.email                AS usuario,
            action_name,
            request_params['table_full_name']  AS data_product_accedido
        FROM system.access.audit
        WHERE service_name = 'unityCatalog'
          AND action_name IN ('getTable','selectFromTable','createTable','alterTable')
          AND request_params['table_full_name'] LIKE '{CATALOG}%'
          AND event_time >= current_timestamp() - INTERVAL 3 HOURS
        ORDER BY event_time DESC
        LIMIT 20
    """)
    df_audit.show(20, truncate=80)
except Exception as _e:
    _msg = str(_e).lower()
    if any(k in _msg for k in ["access_denied", "permission", "not found", "not authorized"]):
        print("[ATENCION] system.access.audit no esta disponible en esta cuenta.")
        print("  Requiere Unity Catalog Enterprise/Premium con System Tables habilitadas.")
        print("  Para habilitar: Account Console > Unity Catalog > Metastore > System Tables > Enable")
        print()
        print("  Lo que registraria en produccion (ultimas 3 horas):")
        print(f"  {'minuto':<20} {'usuario':<30} {'action_name':<20} data_product_accedido")
        print(f"  {'-'*20} {'-'*30} {'-'*20} {'-'*40}")
        print(f"  2024-01-15 11:02     analista@nequi.com.co          getTable             {CATALOG}.pagos.transacciones")
        print(f"  2024-01-15 11:01     pipeline-job-service            createTable          {CATALOG}.riesgo.alertas")
        print(f"  2024-01-15 10:59     riesgo-bot@nequi.com.co         selectFromTable      {CATALOG}.pagos.transacciones")
        print(f"  2024-01-15 10:45     cumplimiento@nequi.com.co       getTable             {CATALOG}.riesgo.v_transacciones")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Resumen del modulo
# MAGIC
# MAGIC En este modulo construiste el primer Data Product de Delfos:
# MAGIC
# MAGIC | Que hiciste | Comando SQL | Por que importa |
# MAGIC |---|---|---|
# MAGIC | Verificar estructura de dominios | `SHOW SCHEMAS` | Principio 1: propiedad por dominio |
# MAGIC | Cargar 2.500 transacciones | `INSERT INTO` via Spark | Datos reales para demostrar conceptos |
# MAGIC | Perfilar el Data Product | `GROUP BY canal / ciudad / hora` | Validar que el dato es correcto antes de publicar |
# MAGIC | Registrar el contrato | `ALTER TABLE SET TBLPROPERTIES` | Principio 2: datos como producto |
# MAGIC | Documentar columnas | `ALTER COLUMN COMMENT` | Catalogo autodescubrible sin documentacion externa |
# MAGIC | Ver el log de Delta | `DESCRIBE HISTORY` | Trazabilidad regulatoria sin sistema adicional |
# MAGIC | Crear vista con RLS | `CREATE VIEW` + `is_member()` | Principio 4: gobernanza federada |
# MAGIC | Time Travel | `VERSION AS OF` | Auditoria SFC: reproducir estado historico |
# MAGIC | Descubrir Data Products | `information_schema.tables` | Principio 3: plataforma self-serve |
# MAGIC | Auditar accesos | `system.access.audit` | SARLAFT: quien accedio a que y cuando |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Reflexion y discusion
# MAGIC
# MAGIC **Reflexion**
# MAGIC
# MAGIC El equipo de clientes quiere analizar comportamiento de pago por ciudad para personalizar la app.
# MAGIC Deberian solicitar acceso directo a `pagos.transacciones` o pedir al equipo de pagos que publique
# MAGIC una vista agregada sin datos individuales de usuario? Que principio de Data Mesh aplica?
# MAGIC
# MAGIC **Discusion**
# MAGIC
# MAGIC Con Data Mesh, el equipo de pagos es dueno de sus transacciones. Un analista de riesgo necesita
# MAGIC un campo nuevo en la tabla. Quien decide si se agrega? En cuanto tiempo? Como se negocia el
# MAGIC cambio sin romper los consumidores existentes del Data Product?
# MAGIC
# MAGIC **Referencia:** [Data Mesh Principles — Zhamak Dehghani](https://martinfowler.com/articles/data-mesh-principles.html)
