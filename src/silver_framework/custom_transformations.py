"""
Custom column-level transformations driven by the YAML `transformations` section.

Each function signature is:
    fn(df: DataFrame, column: str) -> DataFrame

YAML usage:
    transformations:
      - name: convert_to_est     # or the alias EST_time
        column: process_date
      - name: trim_right_zeros
        column: amount
      - name: trim_right_zeros   # params list is also accepted (first element used)
        params: [order_id]

Supported transformations
─────────────────────────
  convert_to_est / EST_time   Convert a UTC timestamp column to America/New_York (EST/EDT)
  trim_right                  Strip trailing whitespace from a string column
  trim_left                   Strip leading whitespace from a string column
  trim_right_zeros            Remove trailing zeros from a decimal/string representation
                              e.g. "1.500" → "1.5", "100.00" → "100", "1.0" → "1"
  trim_left_zeros             Remove leading zeros from a string/numeric representation
                              e.g. "007" → "7", "001.5" → "1.5"
  to_uppercase                Convert string column to upper case
  to_lowercase                Convert string column to lower case
"""

from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver_framework.logger import get_logger

_log = get_logger("custom_transformations")


# ── Individual transformation functions ───────────────────────────────────────

def convert_to_est(df: DataFrame, column: str) -> DataFrame:
    """Convert a UTC timestamp column to Eastern Time (America/New_York).

    Uses from_utc_timestamp so DST is handled automatically —
    the result is EST (UTC-5) in winter and EDT (UTC-4) in summer.
    """
    return df.withColumn(column, F.from_utc_timestamp(F.col(column), "America/New_York"))


def trim_right(df: DataFrame, column: str) -> DataFrame:
    """Strip trailing whitespace from a string column."""
    return df.withColumn(column, F.rtrim(F.col(column)))


def trim_left(df: DataFrame, column: str) -> DataFrame:
    """Strip leading whitespace from a string column."""
    return df.withColumn(column, F.ltrim(F.col(column)))


def trim_right_zeros(df: DataFrame, column: str) -> DataFrame:
    """Remove trailing zeros from a decimal or string representation.

    Examples:  "1.500" → "1.5"   "100.00" → "100"   "1.0" → "1"
    Integers and strings without a decimal point are returned unchanged.
    The column is cast to string; schema enforcement restores the declared type.
    """
    as_str = F.col(column).cast("string")
    stripped = F.regexp_replace(as_str, r"(\.\d*?)0+$", r"\1")   # remove trailing zeros
    clean    = F.regexp_replace(stripped, r"\.$", "")              # remove a lone trailing dot
    result   = F.when(as_str.contains("."), clean).otherwise(as_str)
    return df.withColumn(column, result)


def trim_left_zeros(df: DataFrame, column: str) -> DataFrame:
    """Remove leading zeros from a string or numeric representation.

    Examples:  "007" → "7"   "001.5" → "1.5"   "0" → "0" (single zero preserved)
    The column is cast to string; schema enforcement restores the declared type.
    """
    as_str  = F.col(column).cast("string")
    cleaned = F.regexp_replace(as_str, r"^0+(?=\d)", "")   # remove leading zeros before digits
    return df.withColumn(column, cleaned)


def to_uppercase(df: DataFrame, column: str) -> DataFrame:
    """Convert a string column to upper case."""
    return df.withColumn(column, F.upper(F.col(column)))


def to_lowercase(df: DataFrame, column: str) -> DataFrame:
    """Convert a string column to lower case."""
    return df.withColumn(column, F.lower(F.col(column)))


# ── Registry ─────────────────────────────────────────────────────────────────
# Maps every YAML `name` to its implementation function.
# Add aliases so legacy YAML names keep working alongside canonical names.

_REGISTRY: Dict[str, Any] = {
    "convert_to_est":  convert_to_est,
    "EST_time":        convert_to_est,   # alias used in existing configs
    "trim_right":      trim_right,
    "trim_left":       trim_left,
    "trim_right_zeros": trim_right_zeros,
    "trim_left_zeros": trim_left_zeros,
    "to_uppercase":    to_uppercase,
    "to_lowercase":    to_lowercase,
}


# ── Dispatcher ────────────────────────────────────────────────────────────────

def apply_custom_transformations(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Apply column-level transformations declared in the YAML `transformations` section.

    Each step must specify the target column via either:
      - `column: <col>`           — single column (preferred)
      - `params: [<col>, ...]`    — list format; first element is used as the column

    Unknown transformation names and missing columns are logged as warnings and
    skipped so a misconfigured step never blocks the pipeline.
    """
    steps: List[Dict[str, Any]] = config.get("transformations", [])
    if not steps:
        return df

    for step in steps:
        name: str = step["name"]

        # Resolve target column from either `column` key or first element of `params`
        column = step.get("column") or (step.get("params") or [None])[0]

        if column is None:
            _log.warning("Transformation '%s' has no column specified — skipping", name)
            continue

        fn = _REGISTRY.get(name)
        if fn is None:
            _log.warning("Unknown transformation '%s' — skipping", name)
            continue

        if column not in df.columns:
            _log.warning(
                "Column '%s' not found for transformation '%s' — skipping", column, name
            )
            continue

        _log.info("Applying transformation '%s' to column '%s'", name, column)
        df = fn(df, column)

    return df
