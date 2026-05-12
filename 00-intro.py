# Databricks notebook source
# 00-intro.py — Delfos M1: Fundamentos, Arquitectura y Gobernanza

# COMMAND ----------

# MAGIC %md
# MAGIC # Delfos M1 — Fundamentos, Arquitectura y Gobernanza
# MAGIC ### Nequi · Plataforma de datos
# MAGIC
# MAGIC **Delfos** es la plataforma de datos de Nequi: un conjunto de principios, herramientas
# MAGIC y contratos que permite a cada equipo publicar, consumir y gobernar datos con autonomia,
# MAGIC sin sacrificar la consistencia regulatoria que exige la Superintendencia Financiera de Colombia (SFC).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Agenda
# MAGIC
# MAGIC | Sesión | Titulo | Duracion | Que construyes |
# MAGIC |:---:|---|:---:|---|
# MAGIC | **01** | Data Mesh aplicado a Delfos | 45 min | Primer Data Product del dominio pagos en Unity Catalog |
# MAGIC | **02** | Arquitectura AWS + Databricks + Airflow | 75 min | Pipeline completo Bronze → Silver → Gold ejecutado en vivo |
# MAGIC | **03** | Building Blocks: reutilizar lo existente | 60 min | Extension de building block sin romper contratos existentes |
# MAGIC | **04** | Framework de priorizacion negocio-data | 60 min | Backlog priorizado con RICE y multiplicador regulatorio |
# MAGIC
# MAGIC **Duracion total: 4 horas**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerrequisitos
# MAGIC
# MAGIC - Acceso al workspace de Databricks con Unity Catalog habilitado.
# MAGIC - Bucket S3 aprovisionado (ver carpeta `infraestructura/` en el repositorio).
# MAGIC - Credenciales AWS configuradas como Databricks Secrets en el scope `nequi`.
# MAGIC - Airflow corriendo en la instancia EC2 del ambiente de taller (ver `infraestructura/dags/`).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Como empezar
# MAGIC
# MAGIC 1. Ingresa el nombre de tu bucket S3 en el widget **s3_bucket** (parte superior del notebook).
# MAGIC 2. Abre **01-data-mesh-delfos** en una pestana nueva del workspace.
# MAGIC 3. Haz clic en **Run All** — el notebook `_resource/00-setup` se ejecuta automaticamente en la primera celda.
# MAGIC 4. Sigue el orden de los modulos: **01 → 02 → 03 → 04**.
# MAGIC
# MAGIC > **Nota:** cada modulo comienza con `%run ./_resource/00-setup`, que configura el catalogo,
# MAGIC > las rutas S3 y las funciones compartidas. No es necesario ejecutar el setup por separado.
