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

dbutils.widgets.text("config_path",  "source_configs/sandbox/orders.yml",                                "Config Path")
dbutils.widgets.text("project_root", "/Workspace/Users/alexander.luna@factored.ai/silver-layer-framework", "Project Root")
dbutils.widgets.dropdown("load_mode", "full", ["full", "incremental"], "Load Mode")
dbutils.widgets.text("start_date", "", "Incremental Start Date (YYYY-MM-DD, inclusive)")
dbutils.widgets.text("end_date",   "", "Incremental End Date   (YYYY-MM-DD, inclusive)")

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

config = load_config(full_path)

bronze_table = config["bronze_table"]
silver_table = config["silver_table"]
audit_table  = config["audit_table"]

# Namespace = everything before the table name. Handles both three-part
# (catalog.schema.table) and two-part (schema.table) table names.
bronze_namespace = ".".join(bronze_table.split(".")[:-1])

# ── Read load settings ────────────────────────────────────────────────────────
load_mode  = dbutils.widgets.get("load_mode")
start_date = dbutils.widgets.get("start_date") or None
end_date   = dbutils.widgets.get("end_date")   or None

print(f"bronze_table : {bronze_table}")
print(f"load_mode    : {load_mode}")
print(f"start_date   : {start_date}   end_date: {end_date}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_namespace}")

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
# Full load covers order_ids 1–20 across process_dates 2024-01-15 → 2024-02-03.
# Incremental data starts at order_id 21 and process_date 2024-02-04.
INCREMENTAL_START_ORDER_ID = 21
INCREMENTAL_BASE_DATE      = datetime(2024, 2, 4, 12, 0, 0)

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


def _make_row(order_id, customer_id, raw_amount, status, order_ts, is_deleted_val):
    """Build a single row tuple, appending is_deleted only when soft-delete is active."""
    row = [
        order_id,
        customer_id,
        f"{raw_amount:.2f}",
        status,
        order_ts,
        order_ts.date(),
    ]
    if sd_column:
        row.append(is_deleted_val)
    return tuple(row)


def generate_full_orders():
    """Initial orders dataset used for a full/overwrite load.

    Generates order_ids 1–20 spread across 2024-01-15 → 2024-02-03.
    Scenarios covered:
      - Each order_id appears twice (latest + older duplicate) → dedup keeps latest
      - Every 5th order is soft-deleted
      - DQ failure rows (null order_id / customer_id / amount) → quarantined
    """
    rows     = []
    order_id = 1

    for customer_id in range(1, NUM_CUSTOMERS + 1):
        for i in range(ORDERS_PER_CUSTOMER):
            day_offset = (customer_id - 1) * ORDERS_PER_CUSTOMER + i
            raw_amount = 50.0 + (order_id * 17.5) % 450
            status     = STATUSES[order_id % len(STATUSES)]
            order_ts   = BASE_DATE + timedelta(days=day_offset)
            older_ts   = order_ts - timedelta(days=3)

            rows.append(_make_row(order_id, customer_id, raw_amount,       status,    order_ts, order_id % 5 == 0))
            rows.append(_make_row(order_id, customer_id, raw_amount - 5.0, "pending", older_ts, False))
            order_id += 1

    # DQ failure rows — null required fields
    null_rows = [
        [None,         1,    99.99,  "pending",   BASE_DATE, BASE_DATE.date()],
        [None,         2,    49.99,  "confirmed", BASE_DATE, BASE_DATE.date()],
        [order_id,     None, 120.00, "shipped",   BASE_DATE, BASE_DATE.date()],
        [order_id + 1, None, 200.00, "delivered", BASE_DATE, BASE_DATE.date()],
        [order_id + 2, 3,    None,   "pending",   BASE_DATE, BASE_DATE.date()],
        [order_id + 3, 4,    None,   "cancelled", BASE_DATE, BASE_DATE.date()],
    ]
    for r in null_rows:
        row = [r[0], r[1], f"{r[2]:.2f}" if r[2] is not None else None, r[3], r[4], r[5]]
        if sd_column:
            row.append(False)
        rows.append(tuple(row))

    return spark.createDataFrame(rows, schema_def)


def generate_incremental_orders(proc_start_date: str, proc_end_date: str = None):
    """New orders for an incremental append run, continuing beyond the full-load range.

    order_ids start at INCREMENTAL_START_ORDER_ID (21) so they never overlap with
    the full-load dataset. process_dates start at INCREMENTAL_BASE_DATE (2024-02-04).

    Args:
        proc_start_date: Inclusive start of the incremental window (YYYY-MM-DD).
        proc_end_date:   Inclusive end of the window; open-ended when None.

    Scenarios covered:
      - Each order_id appears twice (latest + older duplicate) → dedup keeps latest
      - Every 5th order is soft-deleted
      - One order per customer, spread across consecutive days from proc_start_date
    """
    start_dt  = date.fromisoformat(proc_start_date)
    end_dt    = date.fromisoformat(proc_end_date) if proc_end_date else None
    rows      = []
    order_id  = INCREMENTAL_START_ORDER_ID

    for customer_id in range(1, NUM_CUSTOMERS + 1):
        day_offset = customer_id - 1
        proc_dt    = start_dt + timedelta(days=day_offset)

        # Skip rows outside the requested window
        if end_dt and proc_dt > end_dt:
            continue

        order_ts   = datetime(proc_dt.year, proc_dt.month, proc_dt.day, 12, 0, 0)
        raw_amount = 50.0 + (order_id * 17.5) % 450
        status     = STATUSES[order_id % len(STATUSES)]
        older_ts   = order_ts - timedelta(days=2)

        rows.append(_make_row(order_id, customer_id, raw_amount,       status,    order_ts, order_id % 5 == 0))
        rows.append(_make_row(order_id, customer_id, raw_amount - 5.0, "pending", older_ts, False))
        order_id += 1

    return spark.createDataFrame(rows, schema_def)

# COMMAND ----------

# load_mode = 'incremental'
# start_data = '2025-01-01'
if load_mode == "full":
    df         = generate_full_orders()
    write_mode = "overwrite"
    print(f"  — {NUM_CUSTOMERS * ORDERS_PER_CUSTOMER} base orders (× 2 with duplicates) + DQ failure rows")
else:
    if not start_date:
        raise ValueError("load_mode=incremental requires start_date to be set")
    df         = generate_incremental_orders(start_date, end_date)
    write_mode = "append"
    print(f"  — incremental window: process_date >= {start_date}" + (f"  AND <= {end_date}" if end_date else ""))

df.write.format("delta").mode(write_mode).saveAsTable(bronze_table)
print(f"Wrote {df.count()} rows to {bronze_table} (mode={write_mode})")

# COMMAND ----------

spark.sql(f"SELECT * FROM {bronze_table} ORDER BY order_id, order_date DESC").display()
