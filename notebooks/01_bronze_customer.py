# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze – Customer Table Setup
# MAGIC
# MAGIC Reads `config_path` to discover the target bronze table name, catalog, and
# MAGIC schema — no hardcoded environment values. Sample data exercises the full
# MAGIC silver pipeline including all custom transformations declared in the YAML:
# MAGIC
# MAGIC | Scenario | Column | Raw value | After transformation |
# MAGIC |---|---|---|---|
# MAGIC | `convert_to_est` | `updated_at` | UTC timestamp | America/New_York (EST/EDT) |
# MAGIC | `trim_left_zeros` | `phone` | `"0123-456-7890"` | `"123-456-7890"` |
# MAGIC | `to_uppercase` | `name` | `"alice"` | `"ALICE"` |
# MAGIC | Deduplication | `customer_id=1` | 2 rows, different timestamps | Latest row kept |
# MAGIC | DQ regex fail | `email` | `"invalid_email"` | Quarantined |
# MAGIC | Soft delete | `is_deleted=True` | `customer_id=3` | Excluded from silver |
# MAGIC | Column normalise | `Email` (CamelCase) | — | Renamed to `email` |
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

bronze_table = config["bronze_table"]
silver_table = config["silver_table"]
audit_table  = config["audit_table"]

bronze_catalog, bronze_schema, _ = bronze_table.split(".")

print(f"bronze_table : {bronze_table}")
print(f"silver_table : {silver_table}")
print(f"audit_table  : {audit_table}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_catalog}.{bronze_schema}")

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

# Timestamps are stored in UTC. convert_to_est subtracts 5h (EST) in January:
#   datetime(2024, 1, 10, 20, 0, 0) UTC → 2024-01-10 15:00:00 EST
#   datetime(2024, 1,  5, 20, 0, 0) UTC → 2024-01-05 15:00:00 EST
#
# phone values with leading zeros test trim_left_zeros:
#   "0123-456-7890" → "123-456-7890"
#
# name values in lowercase test to_uppercase:
#   "alice" → "ALICE"

data = [
    # customer_id=1: duplicate key — latest UTC timestamp wins after dedup
    (1, " valid@email.com ", "alice",   "0123-456-7890", datetime(2024, 1, 10, 20, 0, 0), date(2024, 1, 10), False),
    (1, "old@email.com",     "alice",   "0123-456-7890", datetime(2024, 1,  5, 20, 0, 0), date(2024, 1,  5), False),
    # customer_id=2: invalid email → DQ regex fail → quarantined
    (2, "invalid_email",     "bob",     None,            datetime(2024, 1, 10, 20, 0, 0), date(2024, 1, 10), False),
    # customer_id=3: soft-deleted → excluded from silver output
    (3, "test@test.com",     "charlie", "0789-012-3456", datetime(2024, 1, 10, 20, 0, 0), date(2024, 1, 10), True),
    # customer_id=4: phone has leading zero, name lowercase
    (4, "diana@email.com",   "diana",   "0321-654-0987", datetime(2024, 1, 10, 20, 0, 0), date(2024, 1, 10), False),
    # customer_id=5: phone has no leading zero (trim_left_zeros is a no-op here)
    (5, "eve@email.com",     "eve",     "555-000-1111",  datetime(2024, 1,  9, 20, 0, 0), date(2024, 1,  9), False),
]

df = spark.createDataFrame(data, schema_def)
df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

print(f"Wrote {df.count()} rows to {bronze_table}")

# COMMAND ----------

spark.sql(f"SELECT * FROM {bronze_table} ORDER BY customer_id, updated_at DESC").display()
