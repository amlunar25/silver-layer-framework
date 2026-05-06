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