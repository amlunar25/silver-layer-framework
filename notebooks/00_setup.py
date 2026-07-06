# Databricks notebook source
# MAGIC %md
# MAGIC # Setup – Audit Tables
# MAGIC
# MAGIC One-time setup notebook that creates the two shared audit tables used by the
# MAGIC Silver Layer Framework.  Run this before executing any bronze or silver
# MAGIC notebook for a new environment.
# MAGIC
# MAGIC Table names and the target catalog/schema are read from a single entity YAML
# MAGIC file.  Because all entities in the same project share the same audit tables,
# MAGIC pointing the notebook at **any** entity config is sufficient.
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |-------|---------|
# MAGIC | `audit_table` | DQ audit — one row per entity run (input / valid / invalid counts) |
# MAGIC | `ingestion_audit_table` | Ingestion audit — post-MERGE row-level counts and key reconciliation events |
# MAGIC
# MAGIC The notebook is idempotent: `CREATE TABLE IF NOT EXISTS` means it is safe to
# MAGIC re-run at any time.

# COMMAND ----------

dbutils.widgets.text("config_path",  "source_configs/sandbox/customer.yml",                                "Config Path (any entity)")
dbutils.widgets.text("project_root", "/Workspace/Users/alexander.luna@factored.ai/silver-layer-framework", "Project Root")

# COMMAND ----------

project_root = dbutils.widgets.get("project_root")

# COMMAND ----------

# MAGIC %pip install -q -e $project_root

# COMMAND ----------

import os
project_root = dbutils.widgets.get("project_root")
%load_ext autoreload
%autoreload 2

# COMMAND ----------

import sys
sys.path.append("/Workspace/Users/alexander.luna@factored.ai/silver-layer-framework/src")
from silver_framework.config_loader import load_config

# COMMAND ----------

config_path = dbutils.widgets.get("config_path")
full_path   = config_path if config_path.startswith("/") else os.path.join(project_root, config_path)

config = load_config(full_path)

silver_table          = config["silver_table"]
audit_table           = config["audit_table"]
ingestion_audit_table = config["ingestion_audit_table"]

# Namespace = everything before the table name. Handles both three-part
# (catalog.schema.table) and two-part (schema.table) silver table names.
silver_namespace = ".".join(silver_table.split(".")[:-1])

print(f"silver_namespace      : {silver_namespace}")
print(f"audit_table           : {audit_table}")
print(f"ingestion_audit_table : {ingestion_audit_table}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_namespace}")
print(f"Schema ready: {silver_namespace}")

# COMMAND ----------

# ── DQ Audit Table ────────────────────────────────────────────────────────────
# One record per entity pipeline run — tracks input / valid / invalid counts.
# Schema must match audit_logger._DQ_AUDIT_SCHEMA.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {audit_table} (
    entity        STRING    NOT NULL COMMENT 'Entity name from YAML config',
    silver_table  STRING    NOT NULL COMMENT 'Target silver table',
    input_count   INT       NOT NULL COMMENT 'Total rows read from bronze',
    valid_count   INT       NOT NULL COMMENT 'Rows that passed all DQ checks',
    invalid_count INT       NOT NULL COMMENT 'Rows quarantined by DQ checks',
    status        STRING    NOT NULL COMMENT 'SUCCESS or FAILED',
    timestamp     TIMESTAMP NOT NULL COMMENT 'UTC time of the pipeline run'
)
USING DELTA
COMMENT 'DQ audit log — one record per entity pipeline run'
CLUSTER BY (timestamp)
""")
print(f"DQ audit table ready: {audit_table}")

# COMMAND ----------

# ── Ingestion Audit Table ─────────────────────────────────────────────────────
# One record per MERGE execution and per key-reconciliation run.
# Schema must match audit_logger._INGESTION_AUDIT_SCHEMA.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ingestion_audit_table} (
    entity            STRING    NOT NULL COMMENT 'Entity name from YAML config',
    table_name        STRING    NOT NULL COMMENT 'Target silver table',
    updated_ts        TIMESTAMP NOT NULL COMMENT 'UTC time of the event',
    ingested_records  INT       NOT NULL COMMENT 'Rows sent to the MERGE',
    inserted_records  INT       NOT NULL COMMENT 'New rows added to silver',
    updated_records   INT       NOT NULL COMMENT 'Existing rows updated in silver',
    deleted_records   INT       NOT NULL COMMENT 'Rows removed (soft delete or key reconciliation)',
    status            STRING    NOT NULL COMMENT 'SUCCESS | FAILED | KEY_RECONCILIATION_SUCCESS'
)
USING DELTA
COMMENT 'Ingestion audit log — post-MERGE metrics and key reconciliation events'
CLUSTER BY (updated_ts)
""")
print(f"Ingestion audit table ready: {ingestion_audit_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ Audit

# COMMAND ----------

spark.sql(f"SELECT * FROM {audit_table} ORDER BY timestamp DESC LIMIT 20").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestion Audit

# COMMAND ----------

spark.sql(f"SELECT * FROM {ingestion_audit_table} ORDER BY updated_ts DESC LIMIT 20").display()
