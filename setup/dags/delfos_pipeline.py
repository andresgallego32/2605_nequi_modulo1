# delfos_pipeline.py — DAG de Airflow para el Pipeline Delfos M1
# =============================================================================
# Desplegar en: ~/airflow/dags/delfos_pipeline.py  (en la EC2)
# Orquesta el pipeline Bronze→Silver→Gold del dominio pagos via Databricks.
#
# Prerequisitos en Airflow (configurar antes del workshop):
#   Conexión: databricks_default
#     - Host: https://<workspace>.cloud.databricks.com
#     - Token: dapiXXXXXXXXXXXXXX
#   Variables:
#     - BRONZE_TO_SILVER_JOB_ID  (ID del Job en Databricks)
#     - SILVER_TO_GOLD_JOB_ID    (ID del Job en Databricks)
#     - DELFOS_CATALOG           (default: nequi_prod)
#     - DELFOS_UMBRAL_Z          (default: 3.0)
#     - DELFOS_MAX_TX            (default: 5)
# =============================================================================

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

log = logging.getLogger(__name__)

# ── Leer Variables de Airflow ─────────────────────────────────────────────────
def _var(key: str, default: str) -> str:
    try:
        return Variable.get(key)
    except Exception:
        return default

CATALOG            = _var("DELFOS_CATALOG",            "nequi_prod")
UMBRAL_Z           = _var("DELFOS_UMBRAL_Z",            "3.0")
MAX_TX             = _var("DELFOS_MAX_TX",              "5")
BRONZE_JOB_ID      = _var("BRONZE_TO_SILVER_JOB_ID",   "0")
GOLD_JOB_ID        = _var("SILVER_TO_GOLD_JOB_ID",     "0")

# ── Verificar que los Variables estén configurados ────────────────────────────
def verificar_configuracion(**ctx):
    """Valida que los Job IDs estén configurados antes de ejecutar el pipeline."""
    errores = []
    if BRONZE_JOB_ID == "0":
        errores.append("Variable BRONZE_TO_SILVER_JOB_ID no configurada (es '0')")
    if GOLD_JOB_ID == "0":
        errores.append("Variable SILVER_TO_GOLD_JOB_ID no configurada (es '0')")
    if errores:
        log.error("⛔ Configuración incompleta:\n" + "\n".join(f"  - {e}" for e in errores))
        log.error(
            "Configura las Variables en Airflow UI → Admin → Variables:\n"
            "  BRONZE_TO_SILVER_JOB_ID = <ID del Job Bronze→Silver en Databricks>\n"
            "  SILVER_TO_GOLD_JOB_ID   = <ID del Job Silver→Gold en Databricks>\n"
            "  DELFOS_CATALOG          = nequi_prod\n"
            "  DELFOS_UMBRAL_Z         = 3.0\n"
            "  DELFOS_MAX_TX           = 5"
        )
        raise ValueError("Variables de Airflow no configuradas. Ver logs para detalle.")
    log.info("✅ Configuración válida — catalog=%s umbral_z=%s max_tx=%s", CATALOG, UMBRAL_Z, MAX_TX)
    log.info("   Bronze→Silver Job: %s | Silver→Gold Job: %s", BRONZE_JOB_ID, GOLD_JOB_ID)


def resumen_pipeline(**ctx):
    """Genera el resumen del pipeline al finalizar — útil para el instructor."""
    run_id  = ctx["run_id"]
    dag_run = ctx["dag_run"]
    log.info(
        "\n"
        "╔══════════════════════════════════════════════════╗\n"
        "║  ✅  Delfos Pipeline · Ejecución completada      ║\n"
        "╠══════════════════════════════════════════════════╣\n"
        "║  DAG Run  : %-36s║\n"
        "║  Inicio   : %-36s║\n"
        "║  Catálogo : %-36s║\n"
        "║  Umbral Z : %-36s║\n"
        "║  Max Tx   : %-36s║\n"
        "╚══════════════════════════════════════════════════╝",
        run_id[:36],
        str(dag_run.start_date)[:36],
        CATALOG[:36],
        UMBRAL_Z[:36],
        MAX_TX[:36],
    )


# ── Definición del DAG ────────────────────────────────────────────────────────
default_args = {
    "owner"           : "equipo-datos",
    "depends_on_past" : False,
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry"  : False,
}

with DAG(
    dag_id          = "delfos_pipeline",
    description     = "Delfos M1 — Pipeline pagos Bronze→Silver→Gold (Nequi Data Mesh)",
    schedule        = "0 11 * * *",         # 06:00 Bogotá (UTC-5 → UTC 11:00). schedule_interval deprecado en Airflow 2.4+
    start_date      = datetime(2024, 1, 1),
    catchup         = False,
    max_active_runs = 1,                    # evitar ejecuciones paralelas
    default_args    = default_args,
    tags            = ["delfos", "nequi", "pagos", "data-mesh", "m1"],
    doc_md            = """
## Delfos M1 · Pipeline de pagos

Orquesta el pipeline de datos del dominio **pagos** en tres capas:

| Tarea               | Notebook                                | Capa         |
|---------------------|-----------------------------------------|--------------|
| `bronze_to_silver`  | `02-arquitectura-aws-databricks-airflow`| Bronze→Silver |
| `silver_to_gold`    | `02-arquitectura-aws-databricks-airflow`| Silver→Gold   |

### Configuración
Antes de activar el DAG configurar en **Admin → Variables**:
- `BRONZE_TO_SILVER_JOB_ID` — ID del Job de Databricks
- `SILVER_TO_GOLD_JOB_ID`   — ID del Job de Databricks
- `DELFOS_CATALOG`           — catálogo Unity Catalog (default: `nequi_prod`)
- `DELFOS_UMBRAL_Z`          — umbral z-score para alertas (default: `3.0`)
- `DELFOS_MAX_TX`            — max transacciones en 10 min (default: `5`)

Y en **Admin → Connections**:
- `databricks_default` — Host + Token del workspace de Databricks
    """,
) as dag:

    # 1. Validar configuración antes de disparar Databricks
    t_verificar = PythonOperator(
        task_id         = "verificar_configuracion",
        python_callable = verificar_configuracion,
    )

    # 2. Bronze → Silver: ingesta S3 → Delta via Auto Loader
    t_bronze_silver = DatabricksRunNowOperator(
        task_id            = "bronze_to_silver",
        job_id             = BRONZE_JOB_ID,
        notebook_params    = {
            "catalog"  : CATALOG,
            "reset"    : "No",
        },
        databricks_conn_id = "databricks_default",
        polling_period_seconds = 30,
    )

    # 3. Silver → Gold: reglas de negocio + detección de alertas
    t_silver_gold = DatabricksRunNowOperator(
        task_id            = "silver_to_gold",
        job_id             = GOLD_JOB_ID,
        notebook_params    = {
            "catalog"  : CATALOG,
            "umbral_z" : UMBRAL_Z,
            "max_tx"   : MAX_TX,
        },
        databricks_conn_id = "databricks_default",
        polling_period_seconds = 30,
    )

    # 4. Resumen de ejecución (siempre corre, incluso si algo falla)
    t_resumen = PythonOperator(
        task_id         = "resumen_pipeline",
        python_callable = resumen_pipeline,
        trigger_rule    = "all_done",   # corre aunque upstream falle
    )

    # ── Dependencias: verificar → bronze_silver → silver_gold → resumen ───────
    t_verificar >> t_bronze_silver >> t_silver_gold >> t_resumen
