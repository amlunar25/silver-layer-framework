"""
Key reconciliation — source-driven deletion for silver tables.

Detects silver records whose primary keys no longer exist in the bronze source
and removes or marks them, keeping silver consistent with the latest state of
the source system.  This is the complement to flag-based soft delete: use it
when the source does not emit a deletion flag and you rely on key presence as
the authoritative signal.

Configured as a nested block inside ``soft_delete`` in the entity YAML:

    soft_delete:
      enabled: true
      column: is_deleted      # flag-based delete (omit if not applicable)
      value: true
      key_reconciliation:     # presence of this block enables the key scan
        strategy: mark        # mark | remove
        deleted_flag_column: _is_deleted   # mark strategy only
        frequency:
          type: weekly        # weekly | monthly | specific_date | dates
          day_of_week: monday

Strategies
──────────
mark
    Adds / updates a boolean column (``deleted_flag_column``) to True on stale rows
    via a Delta MERGE UPDATE.  The column is created via ALTER TABLE when absent.
    Downstream queries should filter WHERE _is_deleted IS NULL OR NOT _is_deleted.

remove
    Physically deletes stale rows from the silver table via a Delta MERGE DELETE.
    Use when a historical audit trail of removed keys is not required.

Frequency types
───────────────
weekly        day_of_week (str)   Fires when run_date.weekday() matches the named day.
              Values: monday · tuesday · wednesday · thursday · friday · saturday · sunday
monthly       day_of_month (int)  Fires when run_date.day == day_of_month (1–28).
specific_date date (str)          Fires once on the given YYYY-MM-DD date.
dates         dates (list[str])   Fires on any date in the list.
"""

from datetime import date
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver_framework.logger import get_logger

_log = get_logger("key_reconciliation")

_DAY_MAP: Dict[str, int] = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


# ── Frequency gate ─────────────────────────────────────────────────────────────

def is_scheduled_today(
    kr_config: Dict[str, Any],
    run_date: Optional[date] = None,
) -> bool:
    """Return True when key reconciliation is scheduled to run on *run_date*.

    Accepts the ``key_reconciliation`` sub-dict directly (not the full config).
    Returns False for an empty dict (block absent from YAML) and logs a warning
    for unrecognised frequency types.
    """
    freq      = kr_config.get("frequency", {})
    freq_type = freq.get("type", "")
    today     = run_date or date.today()

    if freq_type == "weekly":
        target = _DAY_MAP.get(freq.get("day_of_week", "monday").lower(), 0)
        return today.weekday() == target

    if freq_type == "monthly":
        return today.day == int(freq.get("day_of_month", 1))

    if freq_type == "specific_date":
        return str(today) == str(freq.get("date", ""))

    if freq_type == "dates":
        return str(today) in [str(d) for d in freq.get("dates", [])]

    _log.warning(
        "soft_delete.key_reconciliation.frequency.type '%s' is not recognised — "
        "job will not run",
        freq_type,
    )
    return False


# ── Core reconciliation ────────────────────────────────────────────────────────

def run_key_reconciliation(
    spark: SparkSession,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """Detect and handle silver rows whose primary keys are absent from bronze.

    Always performs a **full scan** of both tables for key comparison,
    regardless of the entity's incremental extraction mode.

    Reads ``config["soft_delete"]["key_reconciliation"]`` for strategy and
    column settings.

    Returns
    -------
    dict with keys:
        affected_count — number of rows acted upon (0 if nothing to do)
        strategy       — "mark" or "remove"
        skipped        — True when the strategy value is unrecognised
    """
    from delta.tables import DeltaTable

    bronze_table: str       = config["bronze_table"]
    silver_table: str       = config["silver_table"]
    primary_keys: List[str] = config["primary_keys"]
    kr_config               = config["soft_delete"]["key_reconciliation"]
    strategy: str           = kr_config.get("strategy", "mark")

    _log.info(
        "Key reconciliation — bronze='%s'  silver='%s'  keys=%s  strategy='%s'",
        bronze_table, silver_table, primary_keys, strategy,
    )

    # Full-scan PKs from both sides (always full regardless of extraction mode)
    bronze_pk_df = spark.table(bronze_table).select(primary_keys).distinct()
    silver_pk_df = spark.table(silver_table).select(primary_keys).distinct()

    silver_total = silver_pk_df.count()

    # left_anti: silver rows whose PK has no match in bronze
    stale_df    = silver_pk_df.join(bronze_pk_df, on=primary_keys, how="left_anti")
    stale_count = stale_df.count()

    _log.info(
        "Silver total keys=%d  |  stale (absent from bronze)=%d",
        silver_total, stale_count,
    )

    if stale_count == 0:
        return {"affected_count": 0, "strategy": strategy, "skipped": False}

    join_cond = " AND ".join([f"t.{pk} = s.{pk}" for pk in primary_keys])
    delta_tbl  = DeltaTable.forName(spark, silver_table)

    # ── remove strategy ───────────────────────────────────────────────────────
    if strategy == "remove":
        _log.info("Deleting %d stale row(s) from '%s'", stale_count, silver_table)
        (
            delta_tbl.alias("t")
            .merge(stale_df.alias("s"), join_cond)
            .whenMatchedDelete()
            .execute()
        )

    # ── mark strategy ─────────────────────────────────────────────────────────
    elif strategy == "mark":
        flag_col: str = kr_config.get("deleted_flag_column", "_is_deleted")

        # Auto-add the flag column when it does not yet exist in the silver table
        if flag_col not in spark.table(silver_table).columns:
            spark.sql(f"ALTER TABLE {silver_table} ADD COLUMN {flag_col} BOOLEAN")
            _log.info("Added column '%s' to '%s'", flag_col, silver_table)

        _log.info(
            "Marking %d stale row(s) as %s=True in '%s'",
            stale_count, flag_col, silver_table,
        )
        (
            delta_tbl.alias("t")
            .merge(stale_df.alias("s"), join_cond)
            .whenMatchedUpdate(set={f"t.{flag_col}": F.lit(True)})
            .execute()
        )

    else:
        _log.warning(
            "Unknown key_reconciliation strategy '%s' — skipping", strategy
        )
        return {"affected_count": stale_count, "strategy": strategy, "skipped": True}

    return {"affected_count": stale_count, "strategy": strategy, "skipped": False}
