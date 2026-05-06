# Build Instructions: Silver Layer Framework

## Objective
Generate a complete project that implements a **Silver Layer Framework** using:
- PySpark
- Delta Lake
- YAML-driven configuration

---

## Step 1: Create Folder Structure

Create the following structure:

silver-framework/
│
├── configs/
│   └── entities/
│       └── customer.yaml
│
├── notebooks/
│   └── silver_load_notebook.py
│
├── src/
│   └── silver_framework/
│       ├── __init__.py
│       ├── config_loader.py
│       ├── bronze_connector.py
│       ├── silver_connector.py
│       ├── transformations.py
│       ├── dq_framework.py
│       ├── schema_enforcement.py
│       └── audit_logger.py
│
├── tests/
│   └── test_silver_framework.py
│
├── main.py
├── requirements.txt
└── README.md

---

## Step 2: Implement YAML Config

Create `customer.yaml` with:
- schema
- primary keys
- DQ checks
- deduplication rules
- soft delete config

---

## Step 3: Implement Core Modules

### config_loader.py
- Load YAML safely

### bronze_connector.py
- Read Delta table
- Apply:
  - full_scan
  - incremental filtering

### transformations.py
- Column normalization
- Trim strings

### dq_framework.py
- Implement:
  - not_null
  - regex
- Output:
  - valid_df
  - invalid_df

### schema_enforcement.py
- Cast types
- Add missing columns

### silver_connector.py
- Deduplication (window)
- Soft delete filter
- MERGE INTO logic

### audit_logger.py
- Write logs to Delta table

---

## Step 4: Build Notebook

The notebook must:

1. Read widgets:
   - config_path
   - full_scan
   - extraction_start_date
   - extraction_end_date

2. Execute pipeline:
   - Load config
   - Read Bronze
   - Transform
   - DQ checks
   - Schema enforcement
   - Deduplicate
   - Soft delete
   - Upsert

---

## Step 5: Create Local Simulation (`main.py`)

The script must:

- Initialize SparkSession with Delta
- Create sample Bronze DataFrame
- Save it as Delta table
- Load YAML config
- Run full pipeline
- Print:
  - Silver results
  - Quarantine results

---

## Step 6: Add Unit Tests

Create tests for:
- DQ framework
- Schema enforcement
- Deduplication
- Soft delete

---

## Step 7: Requirements File

Include:

pyspark
delta-spark
pyyaml

---

## Step 8: README.md

Explain:
- Architecture
- How to run locally
- How to add new entities
- Example execution

---

## Expected Behavior

Given input:

customer_id | email           | updated_at | is_deleted
1           | valid@email.com | latest     | false
1           | old@email.com   | older      | false
2           | invalid_email   | latest     | false
3           | test@test.com   | latest     | true

Output:

Silver:
- Only latest valid non-deleted records

Quarantine:
- Invalid records

---

## Final Instruction

Generate ALL files completely.
Do NOT skip any file.
Ensure everything is runnable.

---

## Phase 2: Enhancements

### Step 9: Multi-Entity Parallel Execution

Refactor the framework to process multiple entities in a single run:

- Accept a list of config paths (or a directory) as input
- Execute each entity pipeline concurrently using Python `ThreadPoolExecutor`
- Each entity runs its full pipeline independently (Bronze read → DQ → schema → dedup → soft delete → upsert)
- Collect per-entity results and surface failures without stopping other entities
- Update `main.py` to demonstrate running multiple entities in parallel
- Update the Databricks notebook to accept a comma-separated list of config paths

---

### Step 10: Logging, Retry Logic, and Error Handling for Databricks Jobs

Add production-grade resilience to every pipeline stage:

**Structured logging**
- Use Python's `logging` module throughout (not `print`)
- Log at INFO level: stage entry/exit, record counts
- Log at WARNING level: empty DataFrames, skipped DQ checks
- Log at ERROR level: stage failures with full tracebacks
- Include entity name and run timestamp in every log message

**Retry logic**
- Wrap the Bronze read and Silver upsert in a retry decorator
- Configurable: `max_retries` (default 3) and `retry_delay_seconds` (default 5)
- Retry on transient Spark/Delta exceptions; re-raise immediately on non-retryable errors

**Error handling**
- Each pipeline stage must be wrapped in try/except
- On failure: log the error, write a FAILED audit record, and raise
- The parallel executor must catch per-entity failures and continue processing remaining entities
- Final summary must report which entities succeeded and which failed

---

### Step 11: Large-Scale Data Optimizations

Optimize the pipeline for tables with hundreds of millions of rows:

**Partitioning**
- Read Bronze with partition pruning when `full_scan = false` (push filter down)
- Write Silver Delta table partitioned by `process_date`
- Set `spark.sql.files.maxPartitionBytes` and `spark.sql.shuffle.partitions` in the session config

**Caching**
- Cache the Bronze DataFrame after the initial read if it is reused across multiple stages (e.g., DQ + schema enforcement)
- Unpersist after the upsert completes
- Only cache when the DataFrame is used more than once; document the reasoning in code

**Broadcast joins**
- In `dq_framework.py`, when excluding invalid keys from the valid set, use a broadcast join if the invalid key set is small (under the broadcast threshold)
- Make the threshold configurable (default: `spark.sql.autoBroadcastJoinThreshold`)
- Add a helper that decides broadcast vs. shuffle join based on estimated row count