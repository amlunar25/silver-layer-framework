from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver_framework.logger import get_logger

_log = get_logger("dq_framework")


def apply_dq_checks(
    df: DataFrame,
    checks: List[Dict[str, Any]],
    primary_keys: List[str],
    broadcast_threshold: int = 10_000,
) -> Tuple[DataFrame, DataFrame]:
    """
    Apply DQ rules from config. Returns (valid_df, invalid_df).
    invalid_df carries a _dq_failed_check column describing the failure.

    Uses a broadcast join to exclude invalid keys when the invalid set is
    small (≤ broadcast_threshold rows), falling back to a shuffle join
    for large invalid sets.
    """
    if not checks:
        _log.warning("No DQ checks configured — returning all records as valid")
        empty_invalid = df.filter(F.lit(False)).withColumn("_dq_failed_check", F.lit(""))
        return df, empty_invalid

    invalid_parts: List[DataFrame] = []

    for check in checks:
        col_name: str = check["column"]
        check_type: str = check["type"]

        _log.info("Applying DQ check: type='%s' column='%s'", check_type, col_name)

        if check_type == "not_null":
            mask = F.col(col_name).isNull()
        elif check_type == "regex":
            pattern: str = check["pattern"]
            mask = F.col(col_name).isNull() | ~F.col(col_name).rlike(pattern)
        else:
            _log.warning("Unknown DQ check type '%s' — skipping", check_type)
            continue

        flagged = df.filter(mask).withColumn(
            "_dq_failed_check", F.lit(f"{check_type}:{col_name}")
        )
        invalid_parts.append(flagged)

    if not invalid_parts:
        empty_invalid = df.filter(F.lit(False)).withColumn("_dq_failed_check", F.lit(""))
        return df, empty_invalid

    invalid_df = invalid_parts[0]
    for part in invalid_parts[1:]:
        invalid_df = invalid_df.union(part)

    invalid_keys = invalid_df.select(primary_keys).distinct()
    invalid_key_count = invalid_keys.count()

    if invalid_key_count <= broadcast_threshold:
        _log.info(
            "Invalid key count %d ≤ threshold %d — using broadcast join",
            invalid_key_count, broadcast_threshold,
        )
        valid_df = df.join(F.broadcast(invalid_keys), on=primary_keys, how="left_anti")
    else:
        _log.info(
            "Invalid key count %d > threshold %d — using shuffle join",
            invalid_key_count, broadcast_threshold,
        )
        valid_df = df.join(invalid_keys, on=primary_keys, how="left_anti")

    _log.info("DQ complete — valid rows: ~%d, invalid rows: %d", df.count() - invalid_key_count, invalid_key_count)
    return valid_df, invalid_df
