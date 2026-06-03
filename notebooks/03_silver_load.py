# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer – Load Notebook
# MAGIC
# MAGIC Orchestrates the Bronze → Silver pipeline for **customer** and **orders**.
# MAGIC All table references (bronze, silver, audit) are read from the YAML config
# MAGIC files — no catalog or schema widgets needed.
# MAGIC
# MAGIC Each entity has its own `full_scan` toggle and date range, so customer can
# MAGIC run incremental while orders runs a full scan (or vice versa).
# MAGIC
# MAGIC **Run order (automated):** set `run_setup = true` to have this notebook call
# MAGIC `01_bronze_customer` and `02_bronze_orders` first; otherwise run them manually.

# COMMAND ----------

# ── Config paths ──────────────────────────────────────────────────────────────
dbutils.widgets.text("customer_config_path", "configs/entities/customer.yaml", "Customer – Config Path")
dbutils.widgets.text("orders_config_path",   "configs/entities/orders.yaml",   "Orders – Config Path")
dbutils.widgets.text("project_root", "/Workspace/Users/alexander.luna@factored.ai/silver-layer-framework", "Project Root")

# ── Infrastructure ────────────────────────────────────────────────────────────
dbutils.widgets.dropdown("run_setup",          "false", ["true", "false"], "Run Bronze Setup Notebooks")
dbutils.widgets.dropdown("drop_silver_tables", "true",  ["true", "false"], "Drop Silver Tables Before Run")
dbutils.widgets.text("max_workers",             "4",                       "Max Parallel Workers")
dbutils.widgets.dropdown("use_cache",          "false", ["true", "false"], "Use Cache (disable on Serverless)")

# ── Customer scan settings ────────────────────────────────────────────────────
# "config" = use extraction.mode declared in the YAML (recommended default)
# "true"   = force full scan regardless of YAML
# "false"  = force incremental regardless of YAML
dbutils.widgets.dropdown("customer_full_scan", "config", ["config", "true", "false"], "Customer – Full Scan Override")
dbutils.widgets.text("customer_start_date",    "",                                    "Customer – Extraction Start Date (YYYY-MM-DD)")
dbutils.widgets.text("customer_end_date",      "",                                    "Customer – Extraction End Date (YYYY-MM-DD)")

# ── Orders scan settings ──────────────────────────────────────────────────────
dbutils.widgets.dropdown("orders_full_scan", "config", ["config", "true", "false"], "Orders – Full Scan Override")
dbutils.widgets.text("orders_start_date",    "",                                    "Orders – Extraction Start Date (YYYY-MM-DD)")
dbutils.widgets.text("orders_end_date",      "",                                    "Orders – Extraction End Date (YYYY-MM-DD)")

# COMMAND ----------

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict

project_root = dbutils.widgets.get("project_root")
sys.path.insert(0, f"{project_root}/src")

from silver_framework.config_loader import load_config
from silver_framework.pipeline_runner import run_entity
%load_ext autoreload
%autoreload 2

# COMMAND ----------

# ── Resolve a relative config path against the project root ──────────────────
def resolve_config_path(config_path: str) -> str:
    return config_path if config_path.startswith("/") else os.path.join(project_root, config_path)

# COMMAND ----------

# ── Read widgets ──────────────────────────────────────────────────────────────
customer_config_path = dbutils.widgets.get("customer_config_path")
orders_config_path   = dbutils.widgets.get("orders_config_path")

run_setup    = dbutils.widgets.get("run_setup").lower() == "true"
drop_silver  = dbutils.widgets.get("drop_silver_tables").lower() == "true"
max_workers  = int(dbutils.widgets.get("max_workers"))
use_cache    = dbutils.widgets.get("use_cache").lower() == "true"

def _parse_scan_override(value: str):
    """Map widget value to run_entity full_scan argument.
    'config' → None  (entity uses its own YAML extraction.mode)
    'true'   → True  (force full scan)
    'false'  → False (force incremental)
    """
    if value == "config":
        return None
    return value == "true"

customer_full_scan  = _parse_scan_override(dbutils.widgets.get("customer_full_scan"))
customer_start_date = dbutils.widgets.get("customer_start_date") or None
customer_end_date   = dbutils.widgets.get("customer_end_date")   or None

orders_full_scan  = _parse_scan_override(dbutils.widgets.get("orders_full_scan"))
orders_start_date = dbutils.widgets.get("orders_start_date") or None
orders_end_date   = dbutils.widgets.get("orders_end_date")   or None

# ── Load configs to derive table names ───────────────────────────────────────
customer_cfg = load_config(resolve_config_path(customer_config_path))
orders_cfg   = load_config(resolve_config_path(orders_config_path))

def _scan_label(override, cfg):
    if override is None:
        return f"config ({cfg.get('extraction', {}).get('mode', 'full_scan')})"
    return "full_scan" if override else "incremental"

print(f"Customer : table={customer_cfg['bronze_table']}  mode={_scan_label(customer_full_scan, customer_cfg)}  start={customer_start_date}  end={customer_end_date}")
print(f"Orders   : table={orders_cfg['bronze_table']}  mode={_scan_label(orders_full_scan, orders_cfg)}  start={orders_start_date}  end={orders_end_date}")
print(f"Options  : run_setup={run_setup}  drop_silver={drop_silver}  max_workers={max_workers}  use_cache={use_cache}")

# COMMAND ----------

# ── Optionally run bronze setup notebooks ─────────────────────────────────────
# Config path and project root are forwarded so each setup notebook reads the
# same YAML to discover its target tables.
if run_setup:
    notebook_dir = os.path.dirname(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    )
    print("Running 01_bronze_customer …")
    dbutils.notebook.run(
        f"{notebook_dir}/01_bronze_customer",
        timeout_seconds=300,
        arguments={"config_path": customer_config_path, "project_root": project_root},
    )
    print("Running 02_bronze_orders …")
    dbutils.notebook.run(
        f"{notebook_dir}/02_bronze_orders",
        timeout_seconds=300,
        arguments={"config_path": orders_config_path, "project_root": project_root},
    )
    print("Bronze setup complete.")

# COMMAND ----------

# ── Drop silver tables for a clean run (controlled by widget) ─────────────────
# Table names come from the already-loaded configs — no widget needed.
if drop_silver:
    for cfg in (customer_cfg, orders_cfg):
        spark.sql(f"DROP TABLE IF EXISTS {cfg['silver_table']}")
        print(f"Dropped {cfg['silver_table']}")

# COMMAND ----------

# ── Per-entity pipeline configuration ─────────────────────────────────────────
entity_runs: list[Dict[str, Any]] = [
    {
        "config_path":           resolve_config_path(customer_config_path),
        "full_scan":             customer_full_scan,
        "extraction_start_date": customer_start_date,
        "extraction_end_date":   customer_end_date,
    },
    {
        "config_path":           resolve_config_path(orders_config_path),
        "full_scan":             orders_full_scan,
        "extraction_start_date": orders_start_date,
        "extraction_end_date":   orders_end_date,
    },
]

# COMMAND ----------

# ── Run entities in parallel, each with its own scan settings ─────────────────
effective_workers = min(max_workers, len(entity_runs))

with ThreadPoolExecutor(max_workers=effective_workers) as executor:
    futures = {
        executor.submit(
            run_entity,
            spark,
            er["config_path"],
            er["full_scan"],
            er["extraction_start_date"],
            er["extraction_end_date"],
            use_cache,
        ): er["config_path"]
        for er in entity_runs
    }
    results = []
    for future in as_completed(futures):
        result = future.result()
        results.append(result)
        status_label = "SUCCESS" if result["status"] == "SUCCESS" else f"FAILED — {result.get('error')}"
        print(
            f"[{result['entity']}] {status_label} "
            f"| input={result['input_count']}  valid={result['valid_count']}  invalid={result['invalid_count']}"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze – Customer

# COMMAND ----------

spark.sql(f"SELECT * FROM {customer_cfg['bronze_table']} ORDER BY customer_id, updated_at DESC").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver – Customer (expected: customer_id 1, 4, 5 only)

# COMMAND ----------

spark.sql(f"SELECT * FROM {customer_cfg['silver_table']} ORDER BY customer_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze – Orders

# COMMAND ----------

spark.sql(f"SELECT * FROM {orders_cfg['bronze_table']} ORDER BY order_id, order_date DESC").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver – Orders (deduplicated, soft-deleted and DQ-failed rows excluded)

# COMMAND ----------

spark.sql(f"SELECT * FROM {orders_cfg['silver_table']} ORDER BY order_id").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Audit Log

# COMMAND ----------

spark.sql(f"SELECT * FROM {customer_cfg['audit_table']} ORDER BY timestamp DESC").display()
