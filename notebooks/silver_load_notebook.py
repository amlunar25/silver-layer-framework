# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Load Notebook
# MAGIC
# MAGIC Orchestrates the Bronze → Silver pipeline for one or more entities in
# MAGIC parallel, driven entirely by YAML config files.

# COMMAND ----------

dbutils.widgets.text(
    "config_paths",
    "configs/entities/customer.yaml",
    "Config Paths (comma-separated)",
)
dbutils.widgets.dropdown("full_scan", "true", ["true", "false"], "Full Scan")
dbutils.widgets.text("extraction_start_date", "", "Extraction Start Date (YYYY-MM-DD)")
dbutils.widgets.text("extraction_end_date", "", "Extraction End Date (YYYY-MM-DD)")
dbutils.widgets.text("max_workers", "4", "Max Parallel Workers")
dbutils.widgets.dropdown("use_cache", "false", ["true", "false"], "Use Cache (disable on Serverless)")

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Users/alexander.luna@factored.ai/silver-layer-framework/src/")

from silver_framework.pipeline_runner import run_entities_parallel
%load_ext autoreload
%autoreload 2

# COMMAND ----------

# pip install -r /Workspace/Users/alexander.luna@factored.ai/silver-layer-framework/requirements.txt

# COMMAND ----------

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from datetime import date, datetime

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze_sandbox.accelerator;
# MAGIC create schema if not exists silver_sandbox.accelerator;
# MAGIC create schema if not exists gold_sandbox.accelerator;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze_sandbox.test_silver;
# MAGIC create schema if not exists silver_sandbox.test_silver;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_sandbox.test_silver.audit_log (
# MAGIC     entity        STRING    NOT NULL COMMENT 'Entity name from YAML config',
# MAGIC     silver_table  STRING    NOT NULL COMMENT 'Target silver table',
# MAGIC     input_count   INT       NOT NULL,
# MAGIC     valid_count   INT       NOT NULL,
# MAGIC     invalid_count INT       NOT NULL,
# MAGIC     status        STRING    NOT NULL COMMENT 'SUCCESS or FAILED',
# MAGIC     timestamp     TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Pipeline audit log — one record per entity run'
# MAGIC CLUSTER BY (timestamp);

# COMMAND ----------

## Creating test tables:
schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("Email", StringType()),        # camelCase — tests normalization
    StructField("name", StringType()),
    StructField("phone", StringType()),
    StructField("updated_at", TimestampType()),
    StructField("process_date", DateType()),
    StructField("is_deleted", BooleanType()),
])
data = [
    (1, " valid@email.com ", "Alice",   "123-456-7890", datetime(2024, 1, 10), date(2024, 1, 10), False),
    (1, "old@email.com",     "Alice",   "123-456-7890", datetime(2024, 1,  5), date(2024, 1,  5), False),
    (2, "invalid_email",     "Bob",     None,           datetime(2024, 1, 10), date(2024, 1, 10), False),
    (3, "test@test.com",     "Charlie", "789-012-3456", datetime(2024, 1, 10), date(2024, 1, 10), True),
]
df = spark.createDataFrame(data, schema)
df.write.format("delta").mode("overwrite").saveAsTable("bronze_sandbox.test_silver.customer")

# COMMAND ----------

config_paths = [
    p.strip()
    for p in dbutils.widgets.get("config_paths").split(",")
    if p.strip()
]
full_scan: bool = dbutils.widgets.get("full_scan").lower() == "true"

extraction_start_date = dbutils.widgets.get("extraction_start_date") or None
extraction_end_date = dbutils.widgets.get("extraction_end_date") or None
max_workers = int(dbutils.widgets.get("max_workers"))
use_cache: bool = dbutils.widgets.get("use_cache").lower() == "true"

print(f"Entities to process: {config_paths}")
print(f"full_scan={full_scan}  start={extraction_start_date}  end={extraction_end_date}")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists silver_sandbox.test_silver.customer;

# COMMAND ----------

results = run_entities_parallel(
    spark,
    config_paths=config_paths,
    full_scan=full_scan,
    extraction_start_date=extraction_start_date,
    extraction_end_date=extraction_end_date,
    max_workers=max_workers,
    use_cache=use_cache,
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_sandbox.test_silver.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_sandbox.test_silver.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_sandbox.test_silver.audit_log;

# COMMAND ----------


