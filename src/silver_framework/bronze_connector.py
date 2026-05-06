from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from silver_framework.logger import get_logger
from silver_framework.retry import with_retry

_log = get_logger("bronze_connector")


@with_retry(max_retries=3, delay_seconds=5)
def read_bronze(
    spark: SparkSession,
    config: Dict[str, Any],
    full_scan: bool = True,
    extraction_start_date: Optional[str] = None,
    extraction_end_date: Optional[str] = None,
) -> DataFrame:
    """Read from the Bronze Delta table with optional incremental filtering.

    Filters are applied before returning so Spark can push them down to the
    Delta scan and skip irrelevant partitions (partition pruning).
    """
    table = config["bronze_table"]
    entity = config.get("entity", "unknown")
    log = get_logger(entity)

    log.info("Reading Bronze table '%s' (full_scan=%s)", table, full_scan)
    df = spark.table(table)

    if not full_scan:
        if extraction_start_date:
            log.info("Applying filter: process_date >= '%s'", extraction_start_date)
            df = df.filter(f"process_date >= '{extraction_start_date}'")
        if extraction_end_date:
            log.info("Applying filter: process_date <= '%s'", extraction_end_date)
            df = df.filter(f"process_date <= '{extraction_end_date}'")
    else:
        log.info("Full scan — no date filter applied")

    return df
