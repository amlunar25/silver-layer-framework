# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze – Orders Table Setup
# MAGIC
# MAGIC Reads `config_path` to discover the target bronze table name, catalog, and
# MAGIC schema — no hardcoded environment values. Uses loops to generate a realistic
# MAGIC test dataset that exercises the full silver pipeline:
# MAGIC
# MAGIC | Scenario | How it's generated |
# MAGIC |---|---|
# MAGIC | Deduplication | Each order has an older duplicate row with an earlier `order_date` |
# MAGIC | Soft delete | Every 5th order is marked `is_deleted = True` |
# MAGIC | DQ – null `order_id` | 2 injected rows with `order_id = None` |
# MAGIC | DQ – null `customer_id` | 2 injected rows with `customer_id = None` |
# MAGIC | DQ – null `amount` | 2 injected rows with `amount = None` |
# MAGIC
# MAGIC **Called by:** `03_silver_load` via `dbutils.notebook.run()`, or run standalone.

# COMMAND ----------

dbutils.widgets.text("config_path",  "configs/entities/orders.yaml",                                "Config Path")
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

from datetime import date, datetime, timedelta

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

bronze_table = config["bronze_table"]   # e.g. bronze_sandbox.test_silver.orders
silver_table = config["silver_table"]   # e.g. silver_sandbox.test_silver.orders
audit_table  = config["audit_table"]    # e.g. silver_sandbox.test_silver.audit_log

bronze_catalog, bronze_schema, _ = bronze_table.split(".")
silver_catalog, silver_schema, _ = silver_table.split(".")

print(f"bronze_table : {bronze_table}")
print(f"silver_table : {silver_table}")
print(f"audit_table  : {audit_table}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_catalog}.{bronze_schema}")

# COMMAND ----------

STATUSES            = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
NUM_CUSTOMERS       = 5
ORDERS_PER_CUSTOMER = 4
BASE_DATE           = datetime(2024, 1, 15)

schema_def = StructType([
    StructField("order_id",     IntegerType()),
    StructField("customer_id",  IntegerType()),
    StructField("amount",       DoubleType()),
    StructField("status",       StringType()),
    StructField("order_date",   TimestampType()),
    StructField("process_date", DateType()),
    StructField("is_deleted",   BooleanType()),
])

rows = []
order_id = 1

# Main loop — generates valid orders with intentional dedup and soft-delete cases
for customer_id in range(1, NUM_CUSTOMERS + 1):
    for i in range(ORDERS_PER_CUSTOMER):
        amount     = round(50.0 + (order_id * 17.5) % 450, 2)
        status     = STATUSES[order_id % len(STATUSES)]
        order_ts   = BASE_DATE - timedelta(days=i)
        proc_dt    = order_ts.date()
        is_deleted = (order_id % 5 == 0)   # every 5th order is soft-deleted

        # Latest version — dedup keeps this row
        rows.append((order_id, customer_id, amount, status, order_ts, proc_dt, is_deleted))

        # Older duplicate — same order_id, earlier timestamp → dedup discards this row
        older_ts = order_ts - timedelta(days=3)
        rows.append((order_id, customer_id, amount - 5.0, "PENDING", older_ts, older_ts.date(), False))

        order_id += 1

# DQ failure rows — null required fields → quarantined by dq_framework
dq_failures = [
    # null order_id — fails not_null:order_id
    (None,         1,    99.99,  "PENDING",   BASE_DATE, BASE_DATE.date(), False),
    (None,         2,    49.99,  "CONFIRMED", BASE_DATE, BASE_DATE.date(), False),
    # null customer_id — fails not_null:customer_id
    (order_id,     None, 120.00, "SHIPPED",   BASE_DATE, BASE_DATE.date(), False),
    (order_id + 1, None, 200.00, "DELIVERED", BASE_DATE, BASE_DATE.date(), False),
    # null amount — fails not_null:amount
    (order_id + 2, 3,    None,   "PENDING",   BASE_DATE, BASE_DATE.date(), False),
    (order_id + 3, 4,    None,   "CANCELLED", BASE_DATE, BASE_DATE.date(), False),
]
rows.extend(dq_failures)

df = spark.createDataFrame(rows, schema_def)
df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

print(f"Wrote {df.count()} rows to {bronze_table}")
print(f"  — {NUM_CUSTOMERS * ORDERS_PER_CUSTOMER} base orders")
print(f"  — {NUM_CUSTOMERS * ORDERS_PER_CUSTOMER} duplicate rows (older timestamps)")
print(f"  — {len(dq_failures)} DQ failure rows (null fields)")

# COMMAND ----------

spark.sql(f"SELECT * FROM {bronze_table} ORDER BY order_id, order_date DESC").display()
