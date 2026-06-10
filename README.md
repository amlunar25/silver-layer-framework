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
schema_enforcement   ← cast types; add missing cols as null OR warn (YAML flag)
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

## Transformations

Two layers of transformations are applied in Stage 2, before DQ checks:

1. **Base transformations** — applied to every entity automatically, no YAML needed
2. **Custom transformations** — declared per-entity in the YAML `transformations` section

---

### Base transformations

#### Column Name Normalisation (`normalize_column_names`)

Renames every column to `snake_case` using the following rules applied in order:

| Step | Rule | Example |
|------|------|---------|
| 1 | Insert `_` before an uppercase letter that follows a lowercase letter or digit | `customerId` → `customer_Id` |
| 2 | Lowercase the whole name | `customer_Id` → `customer_id` |
| 3 | Replace any character that is not `a-z`, `0-9`, or `_` with `_` | `first-name` → `first_name` |
| 4 | Collapse consecutive underscores and strip leading/trailing ones | `__col__` → `col` |

#### String Trimming (`trim_strings`)

Applies `F.trim()` to every column with `StringType`. Non-string columns (integers, timestamps, booleans, etc.) are left untouched.

---

### Custom transformations

Declared in the YAML `transformations` section. Applied after the base transformations. Each step targets a single column via `column:` or the first element of `params:`.

```yaml
transformations:
  - name: convert_to_est
    column: process_date
  - name: trim_right_zeros
    column: amount
  - name: trim_left_zeros
    params: [order_id]    # params list is also accepted; first element is the column
```

Unknown names and missing columns are logged as warnings and skipped — they never block the pipeline.

#### Available functions

| Name | Alias | Description |
|------|-------|-------------|
| `convert_to_est` | `EST_time` | Convert a UTC `TimestampType` column to Eastern Time (`America/New_York`). Handles DST automatically — EST (UTC-5) in winter, EDT (UTC-4) in summer |
| `trim_right` | — | Strip trailing whitespace from a string column (`F.rtrim`) |
| `trim_left` | — | Strip leading whitespace from a string column (`F.ltrim`) |
| `trim_right_zeros` | — | Remove trailing zeros from a decimal/string representation. `"1.500"` → `"1.5"`, `"100.00"` → `"100"`. No-op on integers without a decimal point |
| `trim_left_zeros` | — | Remove leading zeros from a string/numeric representation. `"007"` → `"7"`, `"001.5"` → `"1.5"` |
| `to_uppercase` | — | Convert a string column to upper case |
| `to_lowercase` | — | Convert a string column to lower case |

> **Type note:** `trim_right_zeros` and `trim_left_zeros` cast the column to `StringType` internally. The schema enforcement step (Stage 4) restores the declared type from the YAML schema.

#### Adding a new transformation

1. Add a function `my_transform(df: DataFrame, column: str) -> DataFrame` to `src/silver_framework/custom_transformations.py`
2. Register it in `_REGISTRY` at the bottom of the same file
3. Reference it by name in any entity's YAML — no other changes required

---

## Data Quality (DQ) Checks

DQ checks are declared per-column in the YAML config under `dq_checks`. Records that fail **any** check are excluded from the silver table and written to a quarantine DataFrame carrying a `_dq_failed_check` column (`<type>:<column>`). Exclusion is done by primary key — if any row for a key fails a check, all rows for that key are quarantined.

### Available check types

#### `not_null`

Flags records where the target column is `null`.

```yaml
dq_checks:
  - column: customer_id
    type: not_null
```

#### `regex`

Flags records where the target column is `null` **or** does not match the provided regular expression pattern.

```yaml
dq_checks:
  - column: email
    type: regex
    pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
```

> **Note:** JSON-escape backslashes in YAML strings (`\\d` not `\d`).

### Quarantine output

| Column | Description |
|--------|-------------|
| All source columns | Original values from the bronze record |
| `_dq_failed_check` | Label of the first failing check: `<type>:<column>` (e.g. `regex:email`, `not_null:customer_id`) |

### Join strategy

| Invalid key count | Join used |
|---|---|
| ≤ 10 000 (default threshold) | Broadcast join — avoids a shuffle for small quarantine sets |
| > 10 000 | Shuffle join |

---

## Soft Delete

Two complementary mechanisms keep silver aligned with the source when records are deleted.

### Flag-based soft delete

The source system emits a boolean deletion flag in every row.  The pipeline reads the flag,
filters matching rows before the MERGE, and they never reach silver.

```yaml
soft_delete:
  enabled: true
  column: is_deleted   # column name in the source table
  value: true          # rows matching this value are excluded
```

Set `enabled: false` (or omit the block entirely) when the source has no deletion flag.

---

### Key reconciliation

Used when the source does **not** provide a deletion flag — deleted records simply stop
appearing in new extracts.  On a configurable schedule the pipeline performs a full PK
scan of both the bronze and silver tables, finds silver rows whose keys are absent from
bronze, and applies the chosen strategy.

```yaml
soft_delete:
  enabled: false          # set true if the source also has a flag column
  column: is_deleted      # required when strategy is mark — reused as the delete signal
  value: true
  key_reconciliation:     # presence of this block enables the periodic key scan
    strategy: mark        # mark | remove  (see below)
    frequency:
      type: weekly        # weekly | monthly | specific_date | dates
      day_of_week: monday
```

#### Strategies

| Strategy | Behaviour | When to use |
|----------|-----------|-------------|
| `mark` | Sets `soft_delete.column = True` on stale rows via Delta MERGE UPDATE.  Reuses the same column as the flag-based filter — one delete signal in silver. | Preserve history; downstream queries filter `WHERE is_deleted IS NULL OR NOT is_deleted` |
| `remove` | Physically deletes stale rows from silver via Delta MERGE DELETE.  No column reference needed. | No audit trail required; smaller silver table |

#### Frequency types

| `type` | Required field | Example value | Trigger condition |
|--------|---------------|--------------|-------------------|
| `weekly` | `day_of_week` | `monday` | Fires every week on the named day |
| `monthly` | `day_of_month` | `1` | Fires on the given day-of-month (1–28) |
| `specific_date` | `date` | `"2025-12-31"` | Fires once on that exact date |
| `dates` | `dates` | `["2025-01-01", "2025-04-01"]` | Fires on any date in the list |

#### How both mechanisms can coexist

Both can be active simultaneously.  Flag-based delete runs every pipeline execution;
key reconciliation runs only on its schedule.

```yaml
soft_delete:
  enabled: true              # flag-based: runs every pipeline run
  column: is_deleted         # mark strategy reuses this column — no extra column
  value: true
  key_reconciliation:        # key scan: add this block to enable, remove to disable
    strategy: mark
    frequency:
      type: weekly
      day_of_week: sunday
```

#### Forcing reconciliation at runtime

Set the `force_key_reconciliation` widget to `true` in notebook `04_silver_entity` to
bypass the frequency schedule and run a reconciliation immediately — useful during
testing or after a large backfill.

---

## Incremental Processing

### YAML configuration

Each entity declares its extraction behaviour in the `extraction` block:

```yaml
extraction:
  mode: incremental      # full_scan | incremental
  filter_column: process_date
```

| Field | Required | Description |
|-------|----------|-------------|
| `mode` | Yes | `full_scan` loads all bronze rows; `incremental` applies a date filter |
| `filter_column` | Yes | Column used to filter rows in incremental mode (typically `process_date` or `updated_at`) |

### Scan mode resolution

The effective scan mode is resolved in this order of priority:

| Priority | Source | When it applies |
|----------|--------|-----------------|
| 1 | Widget override (`true` / `false`) | Explicit override in `03_silver_load` |
| 2 | `extraction.mode` in YAML | Widget is set to `config` (default) |

### Auto-detection of `start_date`

When an entity runs in incremental mode and no `start_date` is provided, the pipeline automatically derives the window start by querying `MAX(filter_column)` from the silver table:

```
incremental mode, no start_date
        │
        ├── silver table exists and has data
        │       └── start_date = MAX(filter_column)   ← incremental run
        │
        └── silver table missing or empty
                └── fall back to full scan             ← first run or reset
```

This means a newly onboarded entity automatically runs a full load on the first execution and switches to incremental on every subsequent run — no manual date management required.

> **Re-processing safety:** `start_date` is applied as `filter_column >= start_date` (inclusive), so records at the boundary are always re-evaluated. Because the silver write uses MERGE INTO, re-processing an existing record is safe — it will be updated in place rather than duplicated.

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

The test suite uses a local `SparkSession` (`local[1]`) — no Databricks environment needed.

---

## Test Suite

### Running the notebooks on Databricks

The three notebooks in `notebooks/` form an end-to-end test of the full pipeline against real Delta tables. Run them in the order below, or let `03_silver_load` call the setup notebooks automatically.

#### Prerequisites

1. Upload the project to your Databricks workspace, e.g.:
   ```
   /Workspace/Users/<your-email>/silver-layer-framework/
   ```
2. Confirm the catalogs referenced in the YAML configs exist and your user has `CREATE SCHEMA` and `CREATE TABLE` privileges on them.

---

#### Step 1 — Create the customer bronze table

Open `notebooks/01_bronze_customer` and set the widgets:

| Widget | Default | Description |
|--------|---------|-------------|
| `config_path` | `configs/entities/customer.yaml` | YAML that defines `bronze_table`, `silver_table`, and `audit_table` |
| `project_root` | `/Workspace/Users/alexander.luna@factored.ai/silver-layer-framework` | Absolute workspace path to the project root |

**Run all cells.** The notebook will:
- Create the bronze and silver schemas if they do not exist
- Create the `audit_log` Delta table if it does not exist
- Write 6 sample rows to `bronze_table` (duplicates, invalid email, soft-deleted record)
- Display the bronze table contents

---

#### Step 2 — Create the orders bronze table

Open `notebooks/02_bronze_orders` and set the widgets:

| Widget | Default | Description |
|--------|---------|-------------|
| `config_path` | `configs/entities/orders.yaml` | YAML that defines the orders tables |
| `project_root` | `/Workspace/Users/alexander.luna@factored.ai/silver-layer-framework` | Absolute workspace path to the project root |

**Run all cells.** The notebook will:
- Create the bronze schema if it does not exist
- Generate orders via a loop: 20 base orders × 2 rows (latest + older duplicate) + 6 DQ failure rows
- Write all rows to `bronze_table`
- Display the bronze table contents

---

#### Step 3 — Run the silver pipeline

Open `notebooks/03_silver_load` and set the widgets:

| Widget | Default | Description |
|--------|---------|-------------|
| `customer_config_path` | `configs/entities/customer.yaml` | Customer entity config |
| `orders_config_path` | `configs/entities/orders.yaml` | Orders entity config |
| `project_root` | `/Workspace/Users/alexander.luna@factored.ai/silver-layer-framework` | Absolute workspace path to the project root |
| `run_setup` | `false` | Set to `true` to call notebooks 01 and 02 automatically before running the pipeline |
| `drop_silver_tables` | `true` | Drop silver tables before the run so each execution starts clean |
| `max_workers` | `4` | Number of parallel threads for entity execution |
| `use_cache` | `false` | Disable on Databricks Serverless (no `cache()` / `persist()` support) |
| `customer_full_scan` | `config` | `config` = use `extraction.mode` from YAML; `true` = force full scan; `false` = force incremental |
| `customer_start_date` | _(empty)_ | Incremental start date for customer (`YYYY-MM-DD`). Leave empty to auto-detect from `MAX(filter_column)` |
| `customer_end_date` | _(empty)_ | Incremental end date for customer (`YYYY-MM-DD`, optional upper bound) |
| `orders_full_scan` | `config` | `config` = use `extraction.mode` from YAML; `true` = force full scan; `false` = force incremental |
| `orders_start_date` | _(empty)_ | Incremental start date for orders (`YYYY-MM-DD`). Leave empty to auto-detect from `MAX(filter_column)` |
| `orders_end_date` | _(empty)_ | Incremental end date for orders (`YYYY-MM-DD`, optional upper bound) |

**Run all cells.** The notebook will:
1. Optionally run notebooks 01 and 02 (when `run_setup = true`), passing `config_path` and `project_root` as arguments
2. Drop the silver tables if `drop_silver_tables = true`
3. Resolve each entity's scan mode (YAML default or widget override) and auto-detect `start_date` from the silver table when not provided
4. Execute the customer and orders pipelines in parallel, each with its own scan settings
5. Display bronze and silver tables for both entities, plus the audit log

#### Expected results after a full-scan run

| Entity | Bronze rows | Silver rows | Quarantined |
|--------|------------|-------------|-------------|
| customer | 6 | 3 (ids 1, 4, 5) | 1 (invalid email) + 1 (soft-deleted) |
| orders | 46 | 16 | 4 (null fields) + 4 (soft-deleted) |

---

### Running the unit tests locally

All tests live in `tests/test_silver_framework.py` and are organized by module.

### Fixture

| Fixture | Scope | Description |
|---------|-------|-------------|
| `spark` | `session` | Single local SparkSession shared across all tests (`local[1]`) |

---

### TestDQFramework

Tests for `silver_framework.dq_framework.apply_dq_checks`.

| Test | Description |
|------|-------------|
| `test_not_null_splits_valid_and_invalid` | Records with a null column go to the invalid DataFrame; non-null records go to valid |
| `test_regex_splits_valid_and_invalid` | Records failing a regex pattern are quarantined; matching records pass |
| `test_multiple_checks_union_failures` | A record failing any single check is quarantined; only records passing all checks are valid |
| `test_empty_checks_returns_all_valid` | When no DQ checks are configured, all records are returned as valid |
| `test_invalid_df_contains_failed_check_column` | Quarantined records include a `_dq_failed_check` column with the label `<type>:<column>` |

---

### TestSchemaEnforcement

Tests for `silver_framework.schema_enforcement.enforce_schema`.

| Test | Description |
|------|-------------|
| `test_cast_string_to_integer` | String column is cast to integer with correct value |
| `test_adds_missing_column_as_null` | Columns declared in the schema but absent from the DataFrame are added as `null` |
| `test_drops_extra_columns` | Columns present in the DataFrame but not declared in the schema are removed |
| `test_preserves_column_order` | Output column order matches the order declared in the schema |

---

### TestDeduplication

Tests for `silver_framework.silver_connector.deduplicate`.

| Test | Description |
|------|-------------|
| `test_keeps_latest_record_per_key` | For duplicate primary keys, only the row with the most recent `updated_at` is kept |
| `test_no_row_num_column_in_output` | The internal `_row_num` window column is not present in the final output |
| `test_single_record_per_key_unchanged` | A DataFrame with no duplicates is returned as-is |

---

### TestSoftDelete

Tests for `silver_framework.silver_connector.apply_soft_delete`.

| Test | Description |
|------|-------------|
| `test_removes_deleted_records` | Rows where the soft-delete column equals the configured value are filtered out |
| `test_disabled_returns_all_records` | When `soft_delete.enabled = false`, no rows are filtered |
| `test_no_soft_delete_config_returns_all_records` | When the `soft_delete` key is absent from the config, all rows are returned |

---

### TestTransformations

Tests for `silver_framework.transformations.apply_transformations`.

| Test | Description |
|------|-------------|
| `test_normalizes_camel_case_columns` | CamelCase column names are converted to `snake_case` |
| `test_trims_string_columns` | Leading and trailing whitespace is removed from string columns |
| `test_non_string_columns_untouched` | Non-string columns (e.g. integers) are not modified |

---

### TestRetry

Tests for `silver_framework.retry.with_retry`.

| Test | Description |
|------|-------------|
| `test_retries_then_succeeds` | A transiently failing function is retried and eventually succeeds |
| `test_raises_after_max_retries` | After exhausting all retries, the original exception is re-raised |
| `test_succeeds_on_first_attempt_no_retry` | A function that succeeds immediately is called exactly once |
| `test_preserves_function_name` | The decorator preserves the wrapped function's `__name__` via `functools.wraps` |

---

## How to Add a New Entity

1. Create `configs/entities/<entity_name>.yaml` following the structure below.
2. No Python changes required.

### YAML schema

```yaml
entity: <entity_name>
bronze_table: <catalog>.<schema>.<table>
silver_table: <catalog>.<schema>.<table>
audit_table: <catalog>.<schema>.audit_log
ingestion_audit_table: <catalog>.<schema>.ingestion_audit_log

extraction:
  mode: full_scan          # full_scan | incremental
  filter_column: process_date  # column used for date filtering in incremental mode

schema:
  - name: <col>
    type: <string|integer|long|double|boolean|timestamp|date>

schema_enforcement:
  add_missing_columns: false  # default false: warn and skip | true: add missing cols as null

primary_keys:
  - <col>

deduplication:
  order_by:
    - column: <col>
      direction: desc   # or asc

soft_delete:
  enabled: true
  column: <flag_column>   # omit column/value when source has no flag
  value: true             # records matching this value are excluded
  key_reconciliation:     # optional — add block to enable; remove to disable
    strategy: mark        # mark (uses soft_delete.column) | remove
    frequency:
      type: weekly        # weekly | monthly | specific_date | dates
      day_of_week: monday

dq_checks:
  - column: <col>
    type: not_null
  - column: <col>
    type: regex
    pattern: "<regex>"
```

### Running the notebook for a new entity

In `03_silver_load`, add the new config path to the relevant widget and set the scan override:

| Widget | Recommended value | Notes |
|--------|-------------------|-------|
| `<entity>_config_path` | `configs/entities/<entity_name>.yaml` | Path to the new YAML |
| `<entity>_full_scan` | `config` | Let the YAML `extraction.mode` decide |
| `<entity>_start_date` | _(empty)_ | Auto-detected from `MAX(filter_column)` on incremental runs |
| `<entity>_end_date` | _(empty)_ | Leave empty unless you need to cap the window |

On the **first run** the silver table will be empty, so auto-detection returns nothing and the pipeline falls back to a full scan automatically.

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
