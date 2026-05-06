from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession


def read_bronze(
    spark: SparkSession,
    config: Dict[str, Any],
    full_scan: bool = True,
    extraction_start_date: Optional[str] = None,
    extraction_end_date: Optional[str] = None,
) -> DataFrame:
    """Read from the Bronze Delta table with optional incremental filtering."""
    table = config["bronze_table"]
    df = spark.table(table)

    if not full_scan:
        if extraction_start_date:
            df = df.filter(f"process_date >= '{extraction_start_date}'")
        if extraction_end_date:
            df = df.filter(f"process_date <= '{extraction_end_date}'")

    return df
