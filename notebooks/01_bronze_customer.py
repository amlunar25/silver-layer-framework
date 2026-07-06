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

dbutils.widgets.text(    "config_path",  "source_configs/sandbox/customer.yml",                                  "Config Path")
dbutils.widgets.text(    "project_root", "/Workspace/Users/alexander.luna@factored.ai/silver-layer-framework", "Project Root")
dbutils.widgets.dropdown("load_mode",    "full", ["full", "incremental"],                                  "Load Mode")
dbutils.widgets.text(    "process_date", "2024-02-01",                                                     "Incremental Process Date (YYYY-MM-DD)")

# COMMAND ----------

project_root = dbutils.widgets.get("project_root")

# COMMAND ----------

# MAGIC %pip install -q -e $project_root

# COMMAND ----------

import os
import sys
sys.path.append(os.path.join(project_root, "src"))
from silver_framework.config_loader import load_config
project_root = dbutils.widgets.get("project_root")
%load_ext autoreload
%autoreload 2

# COMMAND ----------

from datetime import date, datetime, timedelta

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

config = load_config(full_path)

bronze_table = config["bronze_table"]
silver_table = config["silver_table"]
audit_table  = config["audit_table"]

# Namespace = everything before the table name. Handles both three-part
# (catalog.schema.table) and two-part (schema.table) bronze table names.
bronze_namespace = ".".join(bronze_table.split(".")[:-1])

load_mode    = dbutils.widgets.get("load_mode")
process_date = dbutils.widgets.get("process_date") or "2024-02-01"

print(f"bronze_table : {bronze_table}")
print(f"silver_table : {silver_table}")
print(f"audit_table  : {audit_table}")
print(f"load_mode    : {load_mode}")
print(f"process_date : {process_date}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_namespace}")

# COMMAND ----------

schema_def = StructType([
    StructField("customer_id",  IntegerType()),
    StructField("Email",        StringType()),   # CamelCase — tests column normalisation
    StructField("name",         StringType()),
    StructField("phone",        StringType()),
    StructField("updated_at",   TimestampType()),
    StructField("process_date", DateType()),
    StructField("is_deleted",   BooleanType()),
])


def generate_full_customers():
    """Initial customer dataset used for a full/overwrite load.

    Scenarios covered:
      - customer_id=1 : duplicate rows → dedup keeps the latest timestamp
      - customer_id=2 : invalid email  → DQ regex quarantine
      - customer_id=3 : soft-deleted   → excluded from silver
      - customer_id=4 : leading-zero phone + lowercase name → trim_left_zeros / to_uppercase
      - customer_id=5 : clean baseline record
    """
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
        # customer_id=5: clean record, no leading zero on phone
        (5, "eve@email.com",     "eve",     "555-000-1111",  datetime(2024, 1,  9, 20, 0, 0), date(2024, 1,  9), False),
    ]
    return spark.createDataFrame(data, schema_def)


def generate_incremental_customers(proc_date_str: str):
    """New and updated customer records for an incremental append run.

    Scenarios covered:
      - customer_id=1 : email update → silver MERGE updates the existing row
      - customer_id=3 : un-deleted   → soft-delete flag cleared, row re-appears in silver
      - customer_id=6 : brand-new customer
      - customer_id=7 : brand-new customer with leading-zero phone
    """
    proc_dt  = date.fromisoformat(proc_date_str)
    # updated_at is set 1 hour after midnight UTC on the process date
    base_ts  = datetime(proc_dt.year, proc_dt.month, proc_dt.day, 1, 0, 0)
    data = [
        # customer_id=1: email changed — MERGE should update the silver row
        (1, "alice.new@email.com", "alice",   "123-456-7890", base_ts,                        proc_dt, False),
        # customer_id=3: previously soft-deleted, now restored
        (3, "charlie@test.com",    "charlie", "789-012-3456", base_ts + timedelta(minutes=5), proc_dt, False),
        # customer_id=6: new customer, clean record
        (6, "frank@email.com",     "frank",   "0654-321-0987", base_ts + timedelta(minutes=10), proc_dt, False),
        # customer_id=7: new customer, invalid email → DQ quarantine
        (7, "not-an-email",        "grace",   "555-111-2222", base_ts + timedelta(minutes=15), proc_dt, False),
    ]
    return spark.createDataFrame(data, schema_def)

# COMMAND ----------

if load_mode == "full":
    df         = generate_full_customers()
    write_mode = "overwrite"
else:
    df         = generate_incremental_customers(process_date)
    write_mode = "append"

df.write.format("delta").mode(write_mode).saveAsTable(bronze_table)
print(f"Wrote {df.count()} rows to {bronze_table} (mode={write_mode})")

# COMMAND ----------

spark.sql(f"SELECT * FROM {bronze_table} ORDER BY customer_id, updated_at DESC").display()

# COMMAND ----------


