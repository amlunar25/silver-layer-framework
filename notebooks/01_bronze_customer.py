# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze – Customer Table Setup
# MAGIC
# MAGIC Reads `config_path` to discover the target bronze table name, catalog, and
# MAGIC schema — no hardcoded environment values. Sample data exercises the full
# MAGIC silver pipeline:
# MAGIC - Duplicate key (customer_id=1) → deduplication keeps the latest record
# MAGIC - Invalid email (customer_id=2) → quarantined by DQ regex check
# MAGIC - Soft-deleted record (customer_id=3) → excluded by soft-delete filter
# MAGIC - CamelCase column name (`Email`) → normalised to `email` by transformations
# MAGIC - Leading/trailing whitespace in email → trimmed by transformations
# MAGIC
# MAGIC **Called by:** `03_silver_load` via `dbutils.notebook.run()`, or run standalone.

# COMMAND ----------

dbutils.widgets.text("config_path",  "configs/entities/customer.yaml",                              "Config Path")
dbutils.widgets.text("project_root", "/Workspace/Users/alexander.luna@factored.ai/silver-layer-framework", "Project Root")

# COMMAND ----------

import os
import sys

import yaml

project_root = dbutils.widgets.get("project_root")
sys.path.insert(0, f"{project_root}/src")
%load_ext autoreload
%autoreload 2

# COMMAND ----------

from datetime import date, datetime

from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

# ── Load config — table names come from the YAML, not from widgets ────────────
config_path = dbutils.widgets.get("config_path")
full_path   = config_path if config_path.startswith("/") else os.path.join(project_root, config_path)

with open(full_path) as f:
    config = yaml.safe_load(f)

bronze_table = config["bronze_table"]   # e.g. bronze_sandbox.test_silver.customer
silver_table = config["silver_table"]   # e.g. silver_sandbox.test_silver.customer
audit_table  = config["audit_table"]    # e.g. silver_sandbox.test_silver.audit_log

# Derive catalog and schema from the fully-qualified table names in the YAML
bronze_catalog, bronze_schema, _ = bronze_table.split(".")
silver_catalog, silver_schema, _ = silver_table.split(".")

print(f"bronze_table : {bronze_table}")
print(f"silver_table : {silver_table}")
print(f"audit_table  : {audit_table}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_catalog}.{bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_catalog}.{silver_schema}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {audit_table} (
    entity        STRING    NOT NULL COMMENT 'Entity name from YAML config',
    silver_table  STRING    NOT NULL COMMENT 'Target silver table',
    input_count   INT       NOT NULL,
    valid_count   INT       NOT NULL,
    invalid_count INT       NOT NULL,
    status        STRING    NOT NULL COMMENT 'SUCCESS or FAILED',
    timestamp     TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Pipeline audit log — one record per entity run'
CLUSTER BY (timestamp)
""")

# COMMAND ----------

schema_def = StructType([
    StructField("customer_id", IntegerType()),
    StructField("Email",       StringType()),   # CamelCase — tests column normalisation
    StructField("name",        StringType()),
    StructField("phone",       StringType()),
    StructField("updated_at",  TimestampType()),
    StructField("process_date", DateType()),
    StructField("is_deleted",  BooleanType()),
])

data = [
    # Duplicate key — latest timestamp wins after dedup
    (1, " valid@email.com ", "Alice",   "123-456-7890", datetime(2024, 1, 10), date(2024, 1, 10), False),
    (1, "old@email.com",     "Alice",   "123-456-7890", datetime(2024, 1,  5), date(2024, 1,  5), False),
    # Invalid email — fails regex DQ check → quarantine
    (2, "invalid_email",     "Bob",     None,           datetime(2024, 1, 10), date(2024, 1, 10), False),
    # Soft-deleted — excluded from silver output
    (3, "test@test.com",     "Charlie", "789-012-3456", datetime(2024, 1, 10), date(2024, 1, 10), True),
    # Valid records
    (4, "diana@email.com",   "Diana",   "321-654-0987", datetime(2024, 1, 10), date(2024, 1, 10), False),
    (5, "eve@email.com",     "Eve",     "555-000-1111", datetime(2024, 1,  9), date(2024, 1,  9), False),
]

df = spark.createDataFrame(data, schema_def)
df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

print(f"Wrote {df.count()} rows to {bronze_table}")

# COMMAND ----------

spark.sql(f"SELECT * FROM {bronze_table} ORDER BY customer_id, updated_at DESC").display()
