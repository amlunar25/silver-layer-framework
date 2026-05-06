from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_dq_checks(
    df: DataFrame,
    checks: List[Dict[str, Any]],
    primary_keys: List[str],
) -> Tuple[DataFrame, DataFrame]:
    """
    Apply DQ rules from config. Returns (valid_df, invalid_df).
    invalid_df contains a _dq_failed_check column describing the failure.
    """
    if not checks:
        empty_invalid = df.filter(F.lit(False)).withColumn("_dq_failed_check", F.lit(""))
        return df, empty_invalid

    invalid_parts: List[DataFrame] = []

    for check in checks:
        col_name: str = check["column"]
        check_type: str = check["type"]

        if check_type == "not_null":
            mask = F.col(col_name).isNull()
        elif check_type == "regex":
            pattern: str = check["pattern"]
            mask = F.col(col_name).isNull() | ~F.col(col_name).rlike(pattern)
        else:
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
    valid_df = df.join(invalid_keys, on=primary_keys, how="left_anti")

    return valid_df, invalid_df
