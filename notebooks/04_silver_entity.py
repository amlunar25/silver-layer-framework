# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer – Single-Entity Template
# MAGIC
# MAGIC Generic template that runs the full Bronze → Silver pipeline for **any entity**
# MAGIC by pointing `config_path` at its YAML file.  No entity-specific code lives here —
# MAGIC all table names, schema, DQ rules, and transformations are read from the YAML.
# MAGIC
# MAGIC ## Widgets
# MAGIC
# MAGIC | Widget | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `config_path` | `configs/entities/customer.yaml` | Relative or absolute path to the entity YAML |
# MAGIC | `project_root` | `/Workspace/Users/…` | Root of the repo on Databricks |
# MAGIC | `full_scan` | `config` | `config` = use YAML `extraction.mode`; `true` = force full scan; `false` = force incremental |
# MAGIC | `start_date` | _(empty)_ | Incremental start date `YYYY-MM-DD` (auto-detected from silver MAX when blank) |
# MAGIC | `end_date` | _(empty)_ | Incremental end date `YYYY-MM-DD` (open-ended when blank) |
# MAGIC | `drop_silver_table` | `false` | Drop the silver table before running (clean-slate test) |
# MAGIC | `use_cache` | `false` | Enable DataFrame caching (disable on Serverless) |
# MAGIC
# MAGIC ## Typical usage
# MAGIC
# MAGIC - **Interactive run**: fill widgets in the Databricks UI and click *Run All*.
# MAGIC - **Called from notebook 03**: `dbutils.notebook.run("04_silver_entity", 600, {...})`.
# MAGIC - **New entity**: create a YAML file, point `config_path` at it — nothing else changes.

# COMMAND ----------

dbutils.widgets.text(    "config_path",       "configs/entities/customer.yaml",                                "Config Path")
dbutils.widgets.text(    "project_root",      "/Workspace/Users/alexander.luna@factored.ai/silver-layer-framework", "Project Root")
dbutils.widgets.dropdown("full_scan",         "config", ["config", "true", "false"],                         "Full Scan Override")
dbutils.widgets.text(    "start_date",        "",                                                             "Extraction Start Date (YYYY-MM-DD)")
dbutils.widgets.text(    "end_date",          "",                                                             "Extraction End Date   (YYYY-MM-DD)")
dbutils.widgets.dropdown("drop_silver_table", "false",  ["true", "false"],                                   "Drop Silver Table Before Run")
dbutils.widgets.dropdown("use_cache",         "false",  ["true", "false"],                                   "Use Cache (disable on Serverless)")

# COMMAND ----------

import os
import sys

project_root = dbutils.widgets.get("project_root")
sys.path.insert(0, f"{project_root}/src")
%load_ext autoreload
%autoreload 2

# COMMAND ----------

from silver_framework.config_loader import load_config
from silver_framework.pipeline_runner import run_entity

# COMMAND ----------

# ── Resolve widget values ─────────────────────────────────────────────────────
config_path = dbutils.widgets.get("config_path")
full_path   = config_path if config_path.startswith("/") else os.path.join(project_root, config_path)

_scan_raw       = dbutils.widgets.get("full_scan")
start_date      = dbutils.widgets.get("start_date")      or None
end_date        = dbutils.widgets.get("end_date")         or None
drop_silver     = dbutils.widgets.get("drop_silver_table").lower() == "true"
use_cache       = dbutils.widgets.get("use_cache").lower() == "true"

# "config" → None (pipeline_runner reads extraction.mode from YAML)
# "true"   → True  (force full scan)
# "false"  → False (force incremental)
full_scan_override = None if _scan_raw == "config" else (_scan_raw == "true")

# ── Load config to display entity info ───────────────────────────────────────
config = load_config(full_path)

entity       = config["entity"]
bronze_table = config["bronze_table"]
silver_table = config["silver_table"]
audit_table  = config["audit_table"]
ingestion_audit_table = config.get("ingestion_audit_table", "")

_mode_label = (
    f"config ({config.get('extraction', {}).get('mode', 'full_scan')})"
    if full_scan_override is None
    else ("full_scan" if full_scan_override else "incremental")
)

print(f"entity              : {entity}")
print(f"bronze_table        : {bronze_table}")
print(f"silver_table        : {silver_table}")
print(f"audit_table         : {audit_table}")
print(f"ingestion_audit     : {ingestion_audit_table}")
print(f"full_scan_override  : {_mode_label}")
print(f"start_date          : {start_date or '(auto-detect)'}")
print(f"end_date            : {end_date   or '(open)'}")
print(f"drop_silver_table   : {drop_silver}")
print(f"use_cache           : {use_cache}")

# COMMAND ----------

# ── Optionally drop the silver table (clean-slate run) ────────────────────────
if drop_silver:
    spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
    print(f"Dropped silver table: {silver_table}")

# COMMAND ----------

# ── Run the entity pipeline ───────────────────────────────────────────────────
result = run_entity(
    spark,
    config_path       = full_path,
    full_scan         = full_scan_override,
    extraction_start_date = start_date,
    extraction_end_date   = end_date,
    use_cache         = use_cache,
)

status = result["status"]
print(f"\n{'='*60}")
print(f"  Entity  : {result['entity']}")
print(f"  Status  : {status}")
print(f"  Input   : {result['input_count']}")
print(f"  Valid   : {result['valid_count']}")
print(f"  Invalid : {result['invalid_count']}")
if result.get("error"):
    print(f"  Error   : {result['error']}")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Table

# COMMAND ----------

spark.sql(f"SELECT * FROM {bronze_table} LIMIT 100").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Table

# COMMAND ----------

spark.sql(f"SELECT * FROM {silver_table} LIMIT 100").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ Audit Log

# COMMAND ----------

spark.sql(f"""
    SELECT *
    FROM   {audit_table}
    WHERE  entity = '{entity}'
    ORDER  BY timestamp DESC
    LIMIT  20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestion Audit Log

# COMMAND ----------

if ingestion_audit_table:
    spark.sql(f"""
        SELECT *
        FROM   {ingestion_audit_table}
        WHERE  entity = '{entity}'
        ORDER  BY updated_ts DESC
        LIMIT  20
    """).display()
else:
    print("ingestion_audit_table not defined in YAML — skipping")
