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

    full_scan is resolved by pipeline_runner before this call — it already
    reflects the YAML extraction.mode default or an explicit runtime override.

    The filter column is taken from config['extraction']['filter_column'] so
    each entity can declare its own date column.

    Filters are applied before returning so Spark can push them down to the
    Delta scan and skip irrelevant partitions (partition pruning).
    """
    table = config["bronze_table"]
    entity = config.get("entity", "unknown")
    log = get_logger(entity)

    filter_column = config.get("extraction", {}).get("filter_column", "process_date")
    log.info("Reading Bronze table '%s' (full_scan=%s)", table, full_scan)
    df = spark.table(table)

    if not full_scan:
        if extraction_start_date:
            log.info("Applying filter: %s >= '%s'", filter_column, extraction_start_date)
            df = df.filter(f"{filter_column} >= '{extraction_start_date}'")
        if extraction_end_date:
            log.info("Applying filter: %s <= '%s'", filter_column, extraction_end_date)
            df = df.filter(f"{filter_column} <= '{extraction_end_date}'")
    else:
        log.info("Full scan — no date filter applied")

    return df
