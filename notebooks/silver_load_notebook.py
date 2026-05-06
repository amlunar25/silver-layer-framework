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
sys.path.insert(0, "/Workspace/Repos/your-repo/silver-layer-framework")

from silver_framework.pipeline_runner import run_entities_parallel

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

# Summary
print("\n" + "=" * 60)
print("  PIPELINE SUMMARY")
print("=" * 60)
for r in results:
    line = (
        f"  {r['entity']:<25} {r['status']:<8} "
        f"input={r['input_count']}  valid={r['valid_count']}  "
        f"quarantine={r['invalid_count']}"
    )
    if r["error"]:
        line += f"  ERROR: {r['error']}"
    print(line)
print("=" * 60)

succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
failed = len(results) - succeeded
print(f"\n  {succeeded} succeeded / {failed} failed\n")

# Fail the job if any entity failed (so Databricks marks the run as failed)
if failed > 0:
    raise RuntimeError(
        f"{failed} entity pipeline(s) failed: "
        + ", ".join(r["entity"] for r in results if r["status"] == "FAILED")
    )
