# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Load Notebook
# MAGIC
# MAGIC Orchestrates the full Bronze → Silver pipeline for a single entity,
# MAGIC driven entirely by a YAML config file.

# COMMAND ----------

dbutils.widgets.text("config_path", "configs/entities/customer.yaml", "Config Path")
dbutils.widgets.dropdown("full_scan", "true", ["true", "false"], "Full Scan")
dbutils.widgets.text("extraction_start_date", "", "Extraction Start Date (YYYY-MM-DD)")
dbutils.widgets.text("extraction_end_date", "", "Extraction End Date (YYYY-MM-DD)")

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/your-repo/silver-layer-framework")

from silver_framework.config_loader import load_config
from silver_framework.bronze_connector import read_bronze
from silver_framework.transformations import apply_transformations
from silver_framework.dq_framework import apply_dq_checks
from silver_framework.schema_enforcement import enforce_schema
from silver_framework.silver_connector import deduplicate, apply_soft_delete, upsert_to_silver
from silver_framework.audit_logger import log_audit

# COMMAND ----------

config_path: str = dbutils.widgets.get("config_path")
full_scan: bool = dbutils.widgets.get("full_scan").lower() == "true"
extraction_start_date = dbutils.widgets.get("extraction_start_date") or None
extraction_end_date = dbutils.widgets.get("extraction_end_date") or None

config = load_config(config_path)
print(f"Loaded config for entity: {config['entity']}")

# COMMAND ----------

# Step 1 – Read Bronze
raw_df = read_bronze(spark, config, full_scan, extraction_start_date, extraction_end_date)
input_count = raw_df.count()
print(f"Bronze records read: {input_count}")

# COMMAND ----------

# Step 2 – Transform
transformed_df = apply_transformations(raw_df)

# COMMAND ----------

# Step 3 – Data Quality
valid_df, invalid_df = apply_dq_checks(
    transformed_df, config["dq_checks"], config["primary_keys"]
)
valid_count = valid_df.count()
invalid_count = invalid_df.count()
print(f"Valid: {valid_count} | Invalid (quarantine): {invalid_count}")

# COMMAND ----------

# Step 4 – Schema Enforcement
enforced_df = enforce_schema(valid_df, config["schema"])

# COMMAND ----------

# Step 5 – Deduplicate
deduped_df = deduplicate(enforced_df, config)

# COMMAND ----------

# Step 6 – Soft Delete
final_df = apply_soft_delete(deduped_df, config)

# COMMAND ----------

# Step 7 – Upsert to Silver
upsert_to_silver(spark, final_df, config)
print(f"Upserted {final_df.count()} records to {config['silver_table']}")

# COMMAND ----------

# Step 8 – Audit Log
log_audit(
    spark,
    config,
    input_count=input_count,
    valid_count=valid_count,
    invalid_count=invalid_count,
    status="SUCCESS",
)
print("Audit log written.")

# COMMAND ----------

display(final_df)
