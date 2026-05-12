# Delfos M1 — Fundamentos, Arquitectura y Gobernanza

Módulo 1 del programa de formación del equipo de datos de Nequi. Cubre los cuatro pilares
de **Delfos** (la plataforma de datos de Nequi) usando un stack real:
AWS S3 + Databricks + Apache Airflow.

---

## Estructura del proyecto

```
M1-nequi-fundamentos-aquitectura-gobernanza/
│
├── README.md                                    ← este archivo
│
├── delfos-m1-fundamentos/                       ← notebooks Databricks (importar completo)
│   ├── _resource/
│   │   └── 00-setup.py                          ← setup compartido (llamado por %run)
│   ├── setup/
│   │   └── dags/
│   │       └── delfos_pipeline.py               ← copia del DAG para referencia en Databricks
│   ├── 00-intro.py                              ← pantalla de bienvenida
│   ├── 01-data-mesh-delfos.py                   ← Módulo 1 · 45 min
│   ├── 02-arquitectura-aws-databricks-airflow.py ← Módulo 2 · 75 min
│   ├── 03-building-blocks.py                    ← Módulo 3 · 60 min
│   └── 04-framework-priorizacion.py             ← Módulo 4 · 60 min
│
└── infraestructura/                             ← scripts del instructor
    ├── setup_aws.sh                             ← configura toda la infra en un comando (CLI)
    ├── docker-compose.yml                       ← Airflow 2.9 para la EC2
    ├── generate_data.py                         ← genera 15.000 tx sintéticas Nequi
    └── dags/
        └── delfos_pipeline.py                   ← DAG que orquesta el pipeline en Databricks
```

---

## Resumen de la arquitectura

```
AWS EC2 t3.small → Airflow 2.9 (Docker) en :8080
AWS S3 Bucket   → Bronze / Silver / Gold / _checkpoints
Databricks      → Notebooks + Jobs + Unity Catalog

Airflow DAG "delfos_pipeline"
   └── verificar_configuracion
       └── bronze_to_silver  (DatabricksRunNowOperator → Job Databricks)
           └── silver_to_gold (DatabricksRunNowOperator → Job Databricks)
               └── resumen_pipeline
```

---

## SETUP — Solo consola AWS (sin instalar nada en tu laptop)

> Todo se hace desde el navegador. La única "terminal" que vas a ver es **EC2 Instance Connect**,
> que abre una terminal web dentro del navegador de AWS.

---

### BLOQUE 1 — AWS Console (todo con clics)

#### 1.1 Crear el bucket S3

1. Entrar a `console.aws.amazon.com` → buscar **S3** en el buscador
2. Clic en **Create bucket**
3. Completar:
   - **Bucket name:** `nequi-workshop-lga-202501` *(cambia `lga` por tus iniciales)*
   - **Region:** `us-east-1`
   - Todo lo demás dejarlo por defecto
4. Clic en **Create bucket**
5. Entrar al bucket → **Create folder** → crear estas carpetas una por una:
   - `bronze/`
   - `bronze/transacciones/`
   - `silver/`
   - `gold/`
   - `_checkpoints/`

#### 1.2 Crear usuario IAM (para que Databricks acceda a S3)

1. Buscar **IAM** → **Users** → **Create user**
2. **Username:** `databricks-nequi-workshop` → Next
3. **Attach policies directly** → buscar y marcar `AmazonS3FullAccess` → Next → **Create user**
4. Entrar al usuario recién creado → pestaña **Security credentials** → **Create access key**
5. Seleccionar **Application running outside AWS** → Next → **Create access key**
6. **Copiar y guardar el Access key ID y el Secret access key** — los necesitas después

#### 1.3 Lanzar la instancia EC2 para Airflow

1. Buscar **EC2** → **Launch instance**
2. Completar los campos:

| Campo | Valor |
|---|---|
| Name | `nequi-delfos-airflow` |
| AMI | Ubuntu Server 22.04 LTS |
| Instance type | `t3.small` *(2 GB RAM — necesario para Docker + Airflow)* |
| Key pair | Create new key pair → nombre `nequi-key` → RSA → .pem → Create *(se descarga automático)* |
| Firewall | Create security group → marcar **Allow SSH** |

3. En **Advanced details → User data** → pegar este script (instala Docker automáticamente al iniciar):

```bash
#!/bin/bash
apt-get update -y
apt-get install -y docker.io docker-compose-plugin git python3-pip
systemctl start docker
systemctl enable docker
usermod -aG docker ubuntu
mkdir -p /home/ubuntu/airflow/dags /home/ubuntu/airflow/logs /home/ubuntu/airflow/plugins
chown -R ubuntu:ubuntu /home/ubuntu/airflow
```

4. Clic en **Launch instance** → esperar 2–3 min a que diga **Running** y muestre **3/3 comprobaciones**

> **Nota sobre el tipo de instancia:** No uses `t2.micro` ni `t3.micro` (1 GB RAM). Con 1 GB la instancia
> falla los chequeos de estado cuando Docker intenta levantar todos los servicios de Airflow.
> El mínimo recomendado es **t3.small (2 GB RAM)**.

#### 1.4 Abrir el puerto 8080 para la UI de Airflow

1. En EC2 → Instances → clic en tu instancia → pestaña **Security**
2. Clic en el nombre del Security Group (ej. `launch-wizard-2`)
3. Pestaña **Reglas de entrada** → **Editar reglas de entrada** → **Agregar regla**:
   - **Tipo:** TCP personalizado
   - **Intervalo de puertos:** `8080`
   - **Origen:** `Anywhere-IPv4` (pone `0.0.0.0/0`)
4. Clic en **Guardar reglas**

---

### BLOQUE 2 — Instalar Airflow en la EC2 (terminal en el navegador)

#### 2.1 Conectarse a la EC2 desde el navegador

1. EC2 → Instances → seleccionar tu instancia → botón **Conectar** (arriba)
2. Pestaña **EC2 Instance Connect** → clic en **Conectar**
3. Se abre una terminal negra en el navegador con el prompt `ubuntu@ip-XXX:~$`

#### 2.2 Copiar los archivos de Airflow desde GitHub

```bash
# Clonar el repositorio
git clone https://github.com/talentoparati/2605_M1_nequi_fundamentos_aquitectura_gobernanza.git
cd 2605_M1_nequi_fundamentos_aquitectura_gobernanza/infraestructura

# Crear las carpetas que necesita Airflow
mkdir -p ~/airflow/dags ~/airflow/logs ~/airflow/plugins

# Copiar docker-compose y el DAG
cp docker-compose.yml ~/airflow/
cp dags/delfos_pipeline.py ~/airflow/dags/
```

#### 2.3 Crear el archivo de credenciales `.env`

```bash
cat > ~/airflow/.env << 'EOF'
DATABRICKS_HOST=https://adb-XXXXXXXXXXXXXXX.X.azuredatabricks.net
DATABRICKS_TOKEN=dapi...
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=nequi2024
EOF
```

> Reemplaza `DATABRICKS_HOST` y `DATABRICKS_TOKEN` con los valores reales de tu workspace.

#### 2.4 Ajustar permisos y levantar Airflow

```bash
# Ajustar permisos (necesario para que el contenedor pueda escribir logs)
sudo chown -R 50000:0 ~/airflow/logs ~/airflow/dags ~/airflow/plugins

# Ir a la carpeta de Airflow
cd ~/airflow

# Levantar todos los servicios
docker compose --env-file .env up -d
```

Espera 2–3 minutos. Verifica que todos los servicios están corriendo:

```bash
docker compose ps
```

Debes ver algo así:

```
NAME                          STATUS
airflow-postgres-1            healthy
airflow-airflow-init-1        Exited   ← normal, solo corre una vez al inicio
airflow-airflow-webserver-1   healthy
airflow-airflow-scheduler-1   running
```

#### 2.5 Abrir la UI de Airflow

1. Copia la **IP pública** de tu instancia desde EC2 → Instances → Public IPv4
2. Abre en el navegador: `http://<IP-DE-TU-EC2>:8080`
3. Credenciales:
   - **Usuario:** `admin`
   - **Contraseña:** `nequi2024`

> Si la página no carga: verifica que el puerto 8080 está abierto en el Security Group (Bloque 1.4).

#### 2.6 Generar y subir datos sintéticos a S3

```bash
# Volver al repo
cd ~/2605_M1_nequi_fundamentos_aquitectura_gobernanza/infraestructura

# Instalar dependencias
pip3 install boto3 awscli --quiet

# Generar 15.000 transacciones sintéticas
python3 generate_data.py --tx 15000 --days 7 --seed 42

# Configurar credenciales AWS (usar las del usuario IAM creado en el Bloque 1.2)
aws configure set aws_access_key_id AKIAXXXXXXXXXXXXXXXX
aws configure set aws_secret_access_key xxxxxxxxxxxxxxxxxxxxxxxx
aws configure set default.region us-east-1

# Subir a S3 (cambiar por el nombre de tu bucket)
aws s3 sync ./bronze/transacciones/ s3://nequi-workshop-lga-202501/bronze/transacciones/
```

---

### BLOQUE 3 — Databricks Trial en AWS

> **Importante:** Usa el **Trial de Databricks en AWS**, no la Free Edition.
> La Free Edition no tiene clústeres, Jobs ni Secrets — no sirve para este workshop.
>
> Para crear el trial: ve a **databricks.com** → Get started free → selecciona **Amazon Web Services** → región **us-east-1** → completa el registro. El workspace queda listo en ~5 minutos.

#### 3.1 Obtener el Host y Token del workspace

**Host:** Es la URL base del workspace (sin rutas). Ejemplo:
```
https://dbc-XXXXXXXX-XXXX.cloud.databricks.com
```
La encuentras en la barra del navegador — copia solo hasta `.com`.

**Token:**
1. Clic en tu inicial (arriba a la derecha) → **Settings**
2. Menú izquierdo → **Developer**
3. **Access tokens** → **Generate new token**
4. Name: `airflow-workshop` → clic en **Generate**
5. Copia el token (`dapi...`) — **solo se muestra una vez**

> Guarda el Host y el Token — los necesitas para actualizar el `.env` de Airflow y para la conexión en Airflow UI.

#### 3.2 Actualizar el `.env` de Airflow con el workspace Trial

Si ya levantaste Airflow con credenciales de otro workspace, actualiza el `.env` en la EC2:

```bash
cat > ~/airflow/.env << 'EOF'
DATABRICKS_HOST=https://dbc-XXXXXXXX-XXXX.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=nequi2024
EOF

cd ~/airflow && docker compose down && docker compose --env-file .env up -d
```

#### 3.3 Importar los notebooks desde GitHub

1. En Databricks Trial: menú izquierdo → **Workspace**
2. Entra a tu carpeta de usuario
3. Clic en **Create** (arriba a la derecha) → **Git folder**
4. Pega la URL del repositorio:
   ```
   https://github.com/talentoparati/2605_M1_nequi_fundamentos_aquitectura_gobernanza.git
   ```
5. Clic en **Create Git folder** → se crean las carpetas automáticamente

Debe quedar así en tu Workspace:
```
2605_M1_nequi_fundamentos_aquitectura_gobernanza/
├── delfos-m1-fundamentos/
│   ├── _resource/
│   │   └── 00-setup
│   ├── 00-intro
│   ├── 01-data-mesh-delfos
│   ├── 02-arquitectura-aws-databricks-airflow
│   ├── 03-building-blocks
│   └── 04-framework-priorizacion
├── infraestructura/
└── README.md
```

#### 3.4 Crear el clúster

> **Nota:** El Trial de Databricks en AWS usa **Serverless compute** por defecto — no necesitas
> crear un clúster manual. Cuando abras un notebook, en el selector de compute aparece **Serverless**
> como opción. Selecciónala y el notebook corre sin configuración adicional.
>
> Si el workspace sí muestra la pestaña "All-purpose compute" en Compute, puedes crear un clúster así:

| Campo | Valor |
|---|---|
| Cluster name | `delfos-workshop` |
| Policy | Unrestricted |
| Single node | Marcarlo |
| Runtime | `14.3 LTS` |
| Node type | El más pequeño disponible |
| Terminate after | `60` minutos |

#### 3.5 Configurar los Secrets de AWS

Como Databricks no tiene UI para secrets, se configuran desde un notebook temporal que se **borra inmediatamente después**.

**Paso 1 — Crear el notebook en tu carpeta personal (no en el repo):**

1. Menú izquierdo → **Workspace** → **Home** (carpeta de tu usuario)
2. Clic en **Create** → **Notebook**
3. Se crea un notebook en blanco en tu carpeta personal

**Paso 2 — Pegar y ejecutar el código:**

```python
import requests

HOST  = "https://dbc-XXXXXXXX-XXXX.cloud.databricks.com"  # tu workspace Trial
TOKEN = "dapi..."                                           # tu token de Databricks

headers = {"Authorization": f"Bearer {TOKEN}"}

# Crear el scope "nequi"
r = requests.post(f"{HOST}/api/2.0/secrets/scopes/create",
                  headers=headers,
                  json={"scope": "nequi"})
print("Scope:", r.status_code)

# Cargar los 3 secrets (usa POST, no PUT)
for key, value in [
    ("s3-bucket",      "nequi-workshop-lga-202501"),  # nombre de tu bucket S3
    ("aws-access-key", "AKIAXXXXXXXXXXXXXXXX"),         # Access Key ID del usuario IAM
    ("aws-secret-key", "xxxxxxxxxxxxxxxxxxxxxxxx"),     # Secret Access Key del usuario IAM
]:
    r = requests.post(f"{HOST}/api/2.0/secrets/put",
                      headers=headers,
                      json={"scope": "nequi", "key": key, "string_value": value})
    print(f"Secret '{key}':", r.status_code)
```

Conecta a **Serverless** y ejecuta. Debes ver:
```
Scope: 200
Secret 's3-bucket': 200
Secret 'aws-access-key': 200
Secret 'aws-secret-key': 200
```

> Si `secrets/put` devuelve `404`, verifica que estés usando `requests.post` (no `requests.put`).

**Paso 3 — Borrar el notebook inmediatamente:**

Clic derecho sobre el notebook en el panel izquierdo → **Delete** → confirmar.

> El notebook tiene credenciales en texto plano. Borrarlo es obligatorio.

#### 3.6 Probar con el notebook 00-intro

1. En Workspace → abrir `delfos-m1-fundamentos/00-intro`
2. Arriba del notebook, clic en el selector de compute → seleccionar **Serverless**
3. Clic en **Run all**
4. Debe aparecer la pantalla de bienvenida con los 4 módulos y el mensaje:
   ```
   ✅ Delfos · Entorno listo
   S3 Bucket: nequi-workshop-...
   ```

#### 3.7 Crear los 2 Jobs de Databricks (para la integración con Airflow)

**Job 1 — Bronze to Silver:**

1. Menú izquierdo → **Jobs & Pipelines** → **Create job**
2. Completar:
   - **Task name:** `bronze_to_silver`
   - **Type:** Notebook
   - **Notebook path:** navegar hasta `delfos-m1-fundamentos/02-arquitectura-aws-databricks-airflow`
   - **Cluster:** `delfos-workshop`
   - **Parameters (Base parameters):**

| Key | Value |
|---|---|
| `catalog` | `nequi_prod` |
| `reset` | `No` |

3. Clic en **Create** → en la URL aparece el Job ID (ej. `…/jobs/123` → ID es `123`) — **copiarlo**

**Job 2 — Silver to Gold:**

1. Crear un nuevo Job: **Jobs & Pipelines** → **Create job**
2. Completar igual pero con estos parámetros:

| Key | Value |
|---|---|
| `catalog` | `nequi_prod` |
| `umbral_z` | `3.0` |
| `max_tx` | `5` |

3. Clic en **Create** → **copiar el Job ID**

---

### BLOQUE 4 — Airflow UI (todo con clics)

Abrir `http://<IP-EC2>:8080` → usuario `admin` / contraseña `nequi2024`

#### 4.1 Crear la conexión a Databricks

1. **Admin** → **Connections** → botón **+**
2. Completar:

| Campo | Valor |
|---|---|
| Connection Id | `databricks_default` |
| Connection Type | `Databricks` |
| Host | `https://adb-XXXXXXXXXXXXXXX.X.azuredatabricks.net` |
| Extra | `{"token": "dapi..."}` |

3. Clic en **Save**

#### 4.2 Crear las Variables de Airflow

**Admin** → **Variables** → botón **+** → crear estas 5 variables:

| Key | Value |
|---|---|
| `BRONZE_TO_SILVER_JOB_ID` | ID del Job 1 (ej. `123`) |
| `SILVER_TO_GOLD_JOB_ID` | ID del Job 2 (ej. `124`) |
| `DELFOS_CATALOG` | `nequi_prod` |
| `DELFOS_UMBRAL_Z` | `3.0` |
| `DELFOS_MAX_TX` | `5` |

#### 4.3 Activar y correr el DAG

1. Pantalla principal de DAGs → buscar `delfos_pipeline`
2. Activar el **toggle** (pasa de gris a azul)
3. Clic en el botón **▶ (Trigger DAG)**
4. Hacer clic en el nombre del DAG → pestaña **Graph** → ver las 4 tareas ponerse en verde

```
verificar_configuracion → bronze_to_silver → silver_to_gold → resumen_pipeline
```

---

## Orden de los módulos en el workshop

| Módulo | Notebook | Tiempo | Contenido |
|---|---|---|---|
| Intro | `00-intro` | 5 min | Pantalla de bienvenida y agenda |
| **1** | `01-data-mesh-delfos` | **45 min** | 4 principios Data Mesh · Unity Catalog · RLS · Lineage |
| **2** | `02-arquitectura-aws-databricks-airflow` | **75 min** | S3 Medallion · Auto Loader · Bronze→Silver→Gold · Airflow |
| **3** | `03-building-blocks` | **60 min** | Catálogo de componentes · Asset Bundles |
| **4** | `04-framework-priorizacion` | **60 min** | RICE · Multiplicador regulatorio · Roadmap |

**Total: ~4 horas** (con pausas de 10 min entre módulos)

Antes de hacer **Run All** en cualquier módulo:
1. Verificar que el widget `s3_bucket` tiene el nombre correcto del bucket
2. Verificar que el widget `catalog` dice `nequi_prod`
3. Verificar que `reset` dice `No`
4. Conectar el notebook al clúster

---

## Solución de problemas comunes

### La instancia EC2 muestra "2/3 comprobaciones superadas"

La instancia tiene poca RAM (t2.micro o t3.micro con 1 GB). Solución:

1. **Detener** la instancia (no terminar)
2. **Acciones → Configuración de instancia → Cambiar tipo de instancia** → seleccionar `t3.small`
3. **Iniciar** la instancia → esperar a que muestre **3/3 comprobaciones**
4. Copiar la nueva IP pública (cambia cada vez que se inicia la instancia)

### Airflow no abre en el navegador después de `docker compose up`

El webserver tarda ~2 minutos en estar listo. Esperar y refrescar. Si sigue sin cargar:

```bash
docker compose ps
# Verificar que airflow-webserver muestra "healthy"

docker compose logs airflow-webserver --tail=20
# Buscar errores de inicio
```

### Error `PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/...'`

```bash
sudo chown -R 50000:0 ~/airflow/logs ~/airflow/dags ~/airflow/plugins
docker compose down && docker compose --env-file .env up -d
```

### El DAG `delfos_pipeline` no aparece en la UI

```bash
# Verificar que el archivo existe
ls ~/airflow/dags/delfos_pipeline.py

# Ver errores del scheduler
docker compose logs airflow-scheduler | tail -30
```

Si hay un error de importación del proveedor de Databricks, esperar 2-3 minutos — se instala automáticamente al arrancar.

### La UI de Airflow deja de responder mientras corre el DAG

Es un timeout de la sesión de EC2 Instance Connect (la terminal del navegador), no un problema del servidor. Airflow sigue corriendo. Volver a `http://<IP-EC2>:8080` en el navegador.

### El Job de Databricks falla desde Airflow pero corre bien manualmente

El token de Databricks en la conexión de Airflow puede haber expirado.

1. Databricks → **Settings → Developer → Access Tokens → Generate new token**
2. Airflow UI → **Admin → Connections → databricks_default → Edit**
3. Actualizar el campo `Extra`: `{"token": "dapi..."}`

### EC2 no responde en el puerto 8080

Verificar que el Security Group de la instancia tiene el puerto 8080 abierto:

1. EC2 → clic en la instancia → pestaña **Security** → ver el Security Group asociado
2. Entrar al Security Group → **Reglas de entrada**
3. Si no hay regla para el puerto 8080, agregar:
   - Tipo: TCP personalizado | Puerto: 8080 | Origen: Anywhere-IPv4

---

## Limpieza después del workshop

Para no generar costos en AWS:

1. **Detener o terminar la EC2:**
   - EC2 → Instances → seleccionar la instancia → Estado de la instancia → **Detener** (o Terminar)

2. **Vaciar y eliminar el bucket S3:**
   - S3 → seleccionar el bucket → **Empty** → confirmar → **Delete**

3. **Eliminar el usuario IAM:**
   - IAM → Users → `databricks-nequi-workshop` → eliminar access keys → **Delete user**

---

## Referencias

| Tema | Enlace |
|---|---|
| Data Mesh (Zhamak Dehghani) | https://martinfowler.com/articles/data-mesh-principles.html |
| Delta Lake — Medallion Architecture | https://docs.databricks.com/en/lakehouse/medallion.html |
| Databricks Auto Loader | https://docs.databricks.com/en/ingestion/auto-loader/index.html |
| Unity Catalog — Row-Level Security | https://docs.databricks.com/en/data-governance/unity-catalog/row-and-column-filters.html |
| Apache Airflow — DatabricksRunNowOperator | https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators/run_now.html |
