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

_DQ_AUDIT_SCHEMA = StructType([
    StructField("entity",        StringType(),   False),
    StructField("silver_table",  StringType(),   False),
    StructField("input_count",   IntegerType(),  False),
    StructField("valid_count",   IntegerType(),  False),
    StructField("invalid_count", IntegerType(),  False),
    StructField("status",        StringType(),   False),
    StructField("timestamp",     TimestampType(), False),
])

_INGESTION_AUDIT_SCHEMA = StructType([
    StructField("entity",            StringType(),    False),
    StructField("table_name",        StringType(),    False),
    StructField("updated_ts",        TimestampType(), False),
    StructField("ingested_records",  IntegerType(),   False),
    StructField("inserted_records",  IntegerType(),   False),
    StructField("updated_records",   IntegerType(),   False),
    StructField("deleted_records",   IntegerType(),   False),
    StructField("status",            StringType(),    False),
])


def log_dq_audit(
    spark: SparkSession,
    config: Dict[str, Any],
    input_count: int,
    valid_count: int,
    invalid_count: int,
    status: str,
) -> None:
    """Append a DQ audit record (input / valid / invalid counts and pipeline status)."""
    audit_table: str = config["audit_table"]
    entity: str = config.get("entity", "unknown")

    _log.info(
        "Writing DQ audit — entity='%s' status='%s' input=%d valid=%d invalid=%d",
        entity, status, input_count, valid_count, invalid_count,
    )

    row = [(entity, config["silver_table"], input_count, valid_count, invalid_count, status, datetime.utcnow())]
    spark.createDataFrame(row, schema=_DQ_AUDIT_SCHEMA) \
         .write.format("delta").mode("append").saveAsTable(audit_table)


def log_ingestion_audit(
    spark: SparkSession,
    config: Dict[str, Any],
    ingested_records: int,
    inserted_records: int,
    updated_records: int,
    deleted_records: int,
    status: str,
) -> None:
    """Append an ingestion audit record with post-MERGE row-level counts.

    ingested_records — rows sent to the MERGE (after DQ, dedup, soft-delete)
    inserted_records — new rows added to silver (from Delta MERGE metrics)
    updated_records  — existing rows updated in silver (from Delta MERGE metrics)
    deleted_records  — rows removed by the soft-delete filter before the MERGE
    status           — SUCCESS or FAILED
    """
    ingestion_audit_table: str = config["ingestion_audit_table"]
    entity: str = config.get("entity", "unknown")

    _log.info(
        "Writing ingestion audit — entity='%s' status='%s' ingested=%d inserted=%d updated=%d deleted=%d",
        entity, status, ingested_records, inserted_records, updated_records, deleted_records,
    )

    row = [(
        entity,
        config["silver_table"],
        datetime.utcnow(),
        ingested_records,
        inserted_records,
        updated_records,
        deleted_records,
        status,
    )]
    spark.createDataFrame(row, schema=_INGESTION_AUDIT_SCHEMA) \
         .write.format("delta").mode("append").saveAsTable(ingestion_audit_table)
