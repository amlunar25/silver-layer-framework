from datetime import datetime
from typing import Any, Dict

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from silver_framework.logger import get_logger

_log = get_logger("audit_logger")

_AUDIT_SCHEMA = StructType([
    StructField("entity", StringType(), False),
    StructField("silver_table", StringType(), False),
    StructField("input_count", IntegerType(), False),
    StructField("valid_count", IntegerType(), False),
    StructField("invalid_count", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("timestamp", TimestampType(), False),
])


def log_audit(
    spark: SparkSession,
    config: Dict[str, Any],
    input_count: int,
    valid_count: int,
    invalid_count: int,
    status: str,
) -> None:
    """Append a single audit record to the audit Delta table."""
    audit_table: str = config["audit_table"]
    entity: str = config.get("entity", "unknown")

    _log.info(
        "Writing audit record — entity='%s' status='%s' input=%d valid=%d invalid=%d",
        entity, status, input_count, valid_count, invalid_count,
    )

    row = [(
        entity,
        config["silver_table"],
        input_count,
        valid_count,
        invalid_count,
        status,
        datetime.utcnow(),
    )]

    audit_df = spark.createDataFrame(row, schema=_AUDIT_SCHEMA)
    audit_df.write.format("delta").mode("append").saveAsTable(audit_table)
