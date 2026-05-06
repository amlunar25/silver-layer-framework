# Silver Layer Framework

A production-ready, YAML-driven framework for loading data from a Bronze Delta Lake tier into a Silver tier using PySpark and Delta Lake.

---

## Architecture

```
Bronze Delta Table
        │
        ▼
 bronze_connector    ← reads with full/incremental filter
        │
        ▼
  transformations    ← snake_case columns, trim strings
        │
        ▼
  dq_framework       ← not_null / regex checks → splits valid / quarantine
        │
        ▼
schema_enforcement   ← cast types, add missing cols, drop extra cols
        │
        ▼
silver_connector     ← dedup (window), soft delete, MERGE INTO silver
        │
        ▼
  audit_logger       ← writes counts + status to audit Delta table
```

All entity-specific logic (schema, keys, DQ rules, deduplication order,
soft delete column) lives in a single YAML file. No Python changes are
needed to onboard a new entity.

---

## Project Structure

```
silver-layer-framework/
├── configs/
│   └── entities/
│       └── customer.yaml          ← entity config
├── notebooks/
│   └── silver_load_notebook.py    ← Databricks notebook
├── src/
│   └── silver_framework/
│       ├── __init__.py
│       ├── config_loader.py       ← YAML loader
│       ├── bronze_connector.py    ← Bronze Delta reader
│       ├── transformations.py     ← column norm + string trim
│       ├── dq_framework.py        ← not_null / regex DQ
│       ├── schema_enforcement.py  ← cast / add / drop columns
│       ├── silver_connector.py    ← dedup + soft delete + MERGE
│       └── audit_logger.py        ← Delta audit log writer
├── tests/
│   └── test_silver_framework.py
├── main.py                        ← local simulation script
└── requirements.txt
```

---

## How to Run Locally

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

> **Note:** `delta-spark` requires a Java runtime. On macOS:
> `brew install openjdk@11 && export JAVA_HOME=$(brew --prefix openjdk@11)`

### 2. Run the pipeline simulation

```bash
python main.py
```

The script will:
1. Start a local SparkSession with Delta Lake enabled
2. Create and populate a `bronze.customers` Delta table with sample data
3. Run the full pipeline (transform → DQ → schema → dedup → soft delete → MERGE)
4. Print the Silver output and the quarantine output

### 3. Run the test suite

```bash
pytest tests/ -v
```

---

## How to Add a New Entity

1. Create `configs/entities/<entity_name>.yaml` following the structure below.
2. No Python changes required.

### YAML schema

```yaml
entity: <entity_name>
bronze_table: bronze.<table>
silver_table: silver.<table>
audit_table: silver.audit_log

schema:
  - name: <col>
    type: <string|integer|long|double|boolean|timestamp|date>

primary_keys:
  - <col>

deduplication:
  order_by:
    - column: <col>
      direction: desc   # or asc

soft_delete:
  enabled: true
  column: <flag_column>
  value: true           # records matching this value are excluded

dq_checks:
  - column: <col>
    type: not_null
  - column: <col>
    type: regex
    pattern: "<regex>"
```

### Running the notebook for a new entity

In the Databricks widget, set:
- `config_path` → `configs/entities/<entity_name>.yaml`
- `full_scan` → `true` or `false`
- `extraction_start_date` / `extraction_end_date` → for incremental loads

---

## Example Execution

### Input (Bronze)

| customer_id | email           | updated_at | is_deleted |
|-------------|-----------------|------------|------------|
| 1           | valid@email.com | 2024-01-10 | false      |
| 1           | old@email.com   | 2024-01-05 | false      |
| 2           | invalid_email   | 2024-01-10 | false      |
| 3           | test@test.com   | 2024-01-10 | true       |

### Silver output

| customer_id | email           | updated_at | is_deleted |
|-------------|-----------------|------------|------------|
| 1           | valid@email.com | 2024-01-10 | false      |

- Customer 1 deduplicated: latest record kept.
- Customer 2 quarantined: email fails regex check.
- Customer 3 excluded: soft-deleted.

### Quarantine output

| customer_id | email         | _dq_failed_check      |
|-------------|---------------|----------------------|
| 2           | invalid_email | regex:email          |

---

## Technologies

- Python 3.10+
- PySpark ≥ 3.3
- Delta Lake (delta-spark ≥ 2.3)
- PyYAML ≥ 6.0
