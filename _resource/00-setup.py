# Databricks notebook source
# _resource/00-setup.py — Delfos M1: Fundamentos, Arquitectura y Gobernanza
# Invocado por cada modulo con: %run ./_resource/00-setup
# Configura entorno, Unity Catalog y funciones compartidas. NO ejecutar directo.

# COMMAND ----------

# DBTITLE 1,Widgets globales del workshop
import re, json
from datetime import datetime
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window
from typing import List

for _n, _d, _l in [
    ("s3_bucket", "",      "S3 Bucket (vacio = secrets)"),
    ("catalog",   "main",  "Catalogo Unity Catalog (debe existir)"),
    ("dominio",   "pagos", "Dominio de negocio"),
    ("nickname",  "",      "Nickname / iniciales (sufijo de schemas, ej: jdoe)"),
]:
    try:    dbutils.widgets.get(_n)
    except: dbutils.widgets.text(_n, _d, _l)

try: dbutils.widgets.get("reset")
except: dbutils.widgets.dropdown("reset","No",["No","Si — reiniciar datos"],"Reiniciar")

CATALOG = dbutils.widgets.get("catalog").strip() or "main"
DOMINIO = dbutils.widgets.get("dominio").strip() or "pagos"
RESET   = dbutils.widgets.get("reset").startswith("Si")
NICK    = re.sub(r"[^a-z0-9]", "", dbutils.widgets.get("nickname").strip().lower())
assert NICK, "El widget 'nickname' es obligatorio — escribe tus iniciales sin espacios (ej: jdoe)"

# COMMAND ----------

# DBTITLE 1,Credenciales y rutas S3
_bw = dbutils.widgets.get("s3_bucket").strip()
if _bw:
    S3_BUCKET = _bw
else:
    try:    S3_BUCKET = dbutils.secrets.get("nequi", "s3-bucket")
    except: raise ValueError("Escribe el bucket en el widget 's3_bucket' o configura el secret nequi/s3-bucket")

assert re.match(r"^[a-z0-9][a-z0-9\-\.]{2,62}$", S3_BUCKET), f"Bucket invalido: {S3_BUCKET}"

# Unity Catalog External Location maneja las credenciales S3 transparentemente
_CREDS_OK = True

PATH_BRONZE = f"s3://{S3_BUCKET}/bronze/"
PATH_SILVER = f"s3://{S3_BUCKET}/silver/"
PATH_GOLD   = f"s3://{S3_BUCKET}/gold/"
PATH_CKPT   = f"s3://{S3_BUCKET}/_checkpoints/"
PATH_LOGS   = f"s3://{S3_BUCKET}/_audit_logs/"

# COMMAND ----------

# DBTITLE 1,Dominios de Delfos en Unity Catalog
# Cada dominio lleva el sufijo del nickname para aislar schemas por participante
DOMINIOS_DELFOS = {
    f"pagos_{NICK}"    : "Transacciones, pagos QR, transferencias — propietario: equipo-pagos",
    f"riesgo_{NICK}"   : "Modelos antifraude, alertas SARLAFT — propietario: equipo-riesgo",
    f"clientes_{NICK}" : "Perfil y segmentacion de usuarios — propietario: equipo-producto",
    f"canales_{NICK}"  : "App, corresponsal, QR, API — propietario: equipo-canales",
}
SCH_PAGOS    = f"pagos_{NICK}"
SCH_RIESGO   = f"riesgo_{NICK}"
SCH_CLIENTES = f"clientes_{NICK}"
SCH_CANALES  = f"canales_{NICK}"

def _setup_delfos(catalog: str, reset: bool = False) -> None:
    if reset:
        for d in DOMINIOS_DELFOS:
            spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{d} CASCADE")

    spark.sql(f"USE CATALOG {catalog}")

    for dominio, comentario in DOMINIOS_DELFOS.items():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{dominio} COMMENT '{comentario}'")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{SCH_PAGOS}.transacciones (
            transaction_id  STRING    NOT NULL COMMENT 'UUID unico de la transaccion',
            user_id         STRING    NOT NULL COMMENT 'ID usuario (USR####)',
            monto           DOUBLE    COMMENT 'Monto en pesos colombianos COP',
            canal           STRING    COMMENT 'Canal: app | qr | corresponsal | api',
            ciudad          STRING    COMMENT 'Ciudad de origen',
            dispositivo     STRING    COMMENT 'ID dispositivo (DEV###)',
            ts              TIMESTAMP COMMENT 'Timestamp de la transaccion (ISO 8601)',
            capa            STRING    COMMENT 'Capa Medallion: bronze | silver | gold',
            es_fraude_real  BOOLEAN   COMMENT 'Label de verdad — solo para demo/validacion',
            _procesado_en   TIMESTAMP COMMENT 'Timestamp de ingesta por Delfos'
        ) USING DELTA
        PARTITIONED BY (capa)
        COMMENT 'Data Product: transacciones Nequi normalizadas. SLA 1h. SARLAFT.'
    """)

_setup_delfos(CATALOG, reset=RESET)

# COMMAND ----------

# DBTITLE 1,Funciones de utilidad compartidas (disponibles en todos los modulos)

# Calidad de datos
CANALES_VALIDOS = ["app", "qr", "corresponsal", "api"]

def aplicar_reglas_calidad(df: DataFrame) -> DataFrame:
    """Reglas de calidad Delfos: montos > 0, canal valido, ts tipado, sin duplicados."""
    return (df
        .filter(F.col("monto") > 0)
        .withColumn("ts", F.to_timestamp("ts"))
        .filter(F.col("canal").isin(CANALES_VALIDOS))
        .dropDuplicates(["transaction_id"])
        .withColumn("_procesado_en", F.current_timestamp())
    )

# Deteccion de anomalias
def zscore_por_usuario(df: DataFrame, umbral: float = 3.0) -> DataFrame:
    """Building block: z-score por usuario. Reutilizable en cualquier dominio."""
    v = Window.partitionBy("user_id")
    return (df
        .withColumn("_prom", F.avg("monto").over(v))
        .withColumn("_std",  F.stddev("monto").over(v))
        .withColumn("alerta_zscore",
            F.when(F.col("_std").isNotNull() & (F.col("_std") > 0),
                   (F.col("monto") - F.col("_prom")) / F.col("_std") > umbral
            ).otherwise(F.lit(False)))
    )

def frecuencia_ventana(df: DataFrame, ventana_seg: int = 600, max_tx: int = 5) -> DataFrame:
    """Building block: frecuencia en ventana deslizante. Reutilizable por cualquier equipo."""
    v = (Window.partitionBy("user_id")
               .orderBy(F.col("ts").cast("long"))
               .rangeBetween(-ventana_seg, 0))
    return (df
        .withColumn("_tx_ventana",      F.count("*").over(v))
        .withColumn("alerta_frecuencia", F.col("_tx_ventana") > max_tx)
    )

# Funciones de presentacion — imprimen texto plano, no requieren ejecucion para leerse
def modulo_header(num, titulo, subtitulo, icono, c1, c2, tiempo):
    sep = "=" * 70
    print(f"\n{sep}")
    print(f"  MODULO {num}  |  {tiempo}")
    print(f"  {titulo}")
    print(f"  {subtitulo}")
    print(f"{sep}\n")

def nota(html, tipo="info"):
    texto = re.sub(r"<[^>]+>", "", html).strip().replace("\n", " ")
    labels = {"info": "NOTA", "tip": "CONSEJO", "warn": "ATENCION"}
    print(f"\n[{labels.get(tipo, 'NOTA')}] {texto}\n")

def checkpoint(p, d, ref, url):
    def strip(s): return re.sub(r"<[^>]+>", "", s).strip()
    print("\n" + "-" * 70)
    print("REFLEXION")
    print(strip(p))
    print("\nDISCUSION")
    print(strip(d))
    print(f"\nReferencia: {ref}")
    print(f"URL: {url}")
    print("-" * 70 + "\n")

# COMMAND ----------

# DBTITLE 1,Estado del entorno
_creds = "Cargadas desde Databricks Secrets" if _CREDS_OK else "No encontradas — verificar scope 'nequi'"

print("=" * 60)
print("  Delfos — Entorno listo")
print("=" * 60)
print(f"  Plataforma : Delfos (Nequi Data Platform)")
print(f"  Catalogo   : {CATALOG}")
print(f"  Nickname   : {NICK}")
print(f"  Schemas    : {' · '.join(DOMINIOS_DELFOS.keys())}")
print(f"  S3 Bucket  : {S3_BUCKET}")
print(f"  Cred. AWS  : {_creds}")
print(f"  Modulos    : 01 Data Mesh · 02 Arquitectura · 03 Building Blocks · 04 Priorizacion")
print("=" * 60)
