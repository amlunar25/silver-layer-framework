# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze – Orders Table Setup
# MAGIC
# MAGIC Reads `config_path` to discover the target bronze table name, catalog, and
# MAGIC schema — no hardcoded environment values.
# MAGIC
# MAGIC Supports two load modes controlled by the `load_mode` widget:
# MAGIC
# MAGIC | Mode | Write behaviour | Use case |
# MAGIC |------|-----------------|----------|
# MAGIC | `full` | `overwrite` — replaces the entire table | First run / full refresh |
# MAGIC | `incremental` | `append` — adds only rows within the date window | Simulates a daily batch extract |
# MAGIC
# MAGIC Orders are spread across ~20 days so the date filter produces meaningful
# MAGIC subsets for incremental testing.
# MAGIC
# MAGIC **Custom transformation scenarios (from YAML):**
# MAGIC
# MAGIC | Scenario | Column | Raw value | After transformation |
# MAGIC |---|---|---|---|
# MAGIC | `EST_time` | `order_date` | UTC timestamp | America/New_York (EST/EDT) |
# MAGIC | `trim_right_zeros` | `amount` | `"85.00"` | `"85"` → double `85.0` after schema enforcement |
# MAGIC | `to_uppercase` | `status` | `"pending"` | `"PENDING"` |
# MAGIC
# MAGIC **DQ scenarios (full load only):**
# MAGIC - Duplicate `order_id` per customer → deduplication keeps the latest record
# MAGIC - Every 5th order marked `is_deleted = True` → excluded by soft-delete filter
# MAGIC - Rows with null `order_id`, `customer_id`, or `amount` → quarantined
# MAGIC
# MAGIC **Called by:** `03_silver_load` via `dbutils.notebook.run()`, or run standalone.

# COMMAND ----------

dbutils.widgets.text("config_path",  "configs/entities/orders.yaml",                                "Config Path")
dbutils.widgets.text("project_root", "/Workspace/Users/alexander.luna@factored.ai/silver-layer-framework", "Project Root")
dbutils.widgets.dropdown("load_mode", "full", ["full", "incremental"], "Load Mode")
dbutils.widgets.text("start_date", "", "Incremental Start Date (YYYY-MM-DD, inclusive)")
dbutils.widgets.text("end_date",   "", "Incremental End Date   (YYYY-MM-DD, inclusive)")

# COMMAND ----------

pip install pyyaml

# COMMAND ----------

import os
import sys

import yaml

project_root = dbutils.widgets.get("project_root")
sys.path.insert(0, f"{project_root}/src")
%load_ext autoreload
%autoreload 2

# COMMAND ----------

from datetime import date, datetime, timedelta

from pyspark.sql import functions as F
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
silver_catalog, silver_schema, _ = silver_table.split(".")

# ── Read load settings ────────────────────────────────────────────────────────
load_mode  = dbutils.widgets.get("load_mode")
start_date = dbutils.widgets.get("start_date") or None
end_date   = dbutils.widgets.get("end_date")   or None

print(f"bronze_table : {bronze_table}")
print(f"load_mode    : {load_mode}")
print(f"start_date   : {start_date}   end_date: {end_date}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_catalog}.{bronze_schema}")

# COMMAND ----------

# ── Soft-delete column ────────────────────────────────────────────────────────
# Only add is_deleted when the YAML declares a soft_delete configuration.
# If the source system does not provide a delete flag, the column is omitted and
# apply_soft_delete will skip the filter rather than raise an error.
sd_config      = config.get("soft_delete", {})
sd_enabled     = sd_config.get("enabled", False)
sd_column      = sd_config.get("column") if sd_enabled else None

# COMMAND ----------

# Statuses stored lowercase → to_uppercase makes them "PENDING", "CONFIRMED", etc.
STATUSES            = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
NUM_CUSTOMERS       = 5
ORDERS_PER_CUSTOMER = 4
# Anchor date at noon UTC — EST_time shifts order_date 5h back (to 07:00 EST)
# making the timezone conversion clearly visible without changing the date
BASE_DATE           = datetime(2024, 1, 15, 12, 0, 0)

# amount is stored as StringType with explicit trailing zeros to exercise trim_right_zeros:
#   "85.00"  → trim_right_zeros → "85"   → schema enforcement → double 85.0
#   "102.50" → trim_right_zeros → "102.5" → schema enforcement → double 102.5
# order_date (TimestampType) will be converted from UTC to EST by EST_time.

# Build schema conditionally based on whether soft-delete column is needed
fields = [
    StructField("order_id",     IntegerType()),
    StructField("customer_id",  IntegerType()),
    StructField("amount",       StringType()),   # stored as string to exercise trim_right_zeros
    StructField("status",       StringType()),   # stored lowercase to exercise to_uppercase
    StructField("order_date",   TimestampType()),
    StructField("process_date", DateType()),
]
if sd_column:
    fields.append(StructField(sd_column, BooleanType()))

schema_def = StructType(fields)

rows = []
order_id = 1

# Spread orders across ~20 days so incremental date windows return distinct subsets.
# customer 1 → 2024-01-15 .. 2024-01-18  (BASE_DATE + 0..3)
# customer 2 → 2024-01-19 .. 2024-01-22
# customer 3 → 2024-01-23 .. 2024-01-26
# customer 4 → 2024-01-27 .. 2024-01-30
# customer 5 → 2024-01-31 .. 2024-02-03
for customer_id in range(1, NUM_CUSTOMERS + 1):
    for i in range(ORDERS_PER_CUSTOMER):
        day_offset = (customer_id - 1) * ORDERS_PER_CUSTOMER + i
        # Format amount as string with 2 decimal places — some end in ".00" or ".50"
        raw_amount = 50.0 + (order_id * 17.5) % 450
        amount     = f"{raw_amount:.2f}"          # e.g. "85.00", "102.50", "120.00"
        status     = STATUSES[order_id % len(STATUSES)]
        order_ts   = BASE_DATE + timedelta(days=day_offset)
        proc_dt    = order_ts.date()

        latest_row = [order_id, customer_id, amount, status, order_ts, proc_dt]
        older_ts   = order_ts - timedelta(days=3)
        older_amt  = f"{raw_amount - 5.0:.2f}"
        older_row  = [order_id, customer_id, older_amt, "pending", older_ts, older_ts.date()]

        if sd_column:
            is_deleted = (order_id % 5 == 0)   # every 5th order soft-deleted
            latest_row.append(is_deleted)
            older_row.append(False)

        rows.append(tuple(latest_row))
        rows.append(tuple(older_row))   # older duplicate — dedup discards this

        order_id += 1

# DQ failure rows (null required fields) — only added on full load
dq_failures = []
if load_mode == "full":
    null_rows = [
        # null order_id
        [None,         1,    "99.99",  "pending",   BASE_DATE, BASE_DATE.date()],
        [None,         2,    "49.99",  "confirmed", BASE_DATE, BASE_DATE.date()],
        # null customer_id
        [order_id,     None, "120.00", "shipped",   BASE_DATE, BASE_DATE.date()],
        [order_id + 1, None, "200.00", "delivered", BASE_DATE, BASE_DATE.date()],
        # null amount
        [order_id + 2, 3,    None,     "pending",   BASE_DATE, BASE_DATE.date()],
        [order_id + 3, 4,    None,     "cancelled", BASE_DATE, BASE_DATE.date()],
    ]
    for r in null_rows:
        if sd_column:
            r.append(False)
        dq_failures.append(tuple(r))

rows.extend(dq_failures)

# COMMAND ----------

df = spark.createDataFrame(rows, schema_def)

# ── Apply incremental date filter ─────────────────────────────────────────────
if load_mode == "incremental":
    if not start_date:
        raise ValueError("load_mode=incremental requires start_date to be set")
    df = df.filter(F.col("process_date") >= F.lit(start_date))
    if end_date:
        df = df.filter(F.col("process_date") <= F.lit(end_date))

write_mode = "overwrite" if load_mode == "full" else "append"
df.write.format("delta").mode(write_mode).saveAsTable(bronze_table)

print(f"Wrote {df.count()} rows to {bronze_table} (mode={write_mode})")
if load_mode == "incremental":
    print(f"  — date filter: process_date >= {start_date}" + (f"  AND <= {end_date}" if end_date else ""))
else:
    print(f"  — {NUM_CUSTOMERS * ORDERS_PER_CUSTOMER} base orders (× 2 with duplicates)")
    print(f"  — {len(dq_failures)} DQ failure rows (null fields)")

# COMMAND ----------

spark.sql(f"SELECT * FROM {bronze_table} ORDER BY order_id, order_date DESC").display()
