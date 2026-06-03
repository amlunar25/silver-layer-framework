from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from silver_framework.logger import get_logger
from silver_framework.retry import with_retry
from silver_framework.schema_enforcement import _TYPE_MAP

_log = get_logger("silver_connector")


def _schema_from_config(schema_config: List[Dict[str, Any]]) -> StructType:
    """Build a Spark StructType from the YAML schema definition."""
    from pyspark.sql.types import StringType
    fields = [
        StructField(f["name"], _TYPE_MAP.get(f["type"].lower(), StringType()), True)
        for f in schema_config
    ]
    return StructType(fields)


def ensure_silver_table(spark: SparkSession, config: Dict[str, Any]) -> None:
    """Create the silver Delta table if it does not already exist.

    Uses spark.catalog.tableExists() instead of DeltaTable.isDeltaTable() to
    avoid the low-level file scan that requires SELECT on any file — a
    privilege Unity Catalog does not grant to regular users.

    Uses the schema defined in the YAML config so the table is always
    created with the correct column types before the first MERGE runs.
    Partitions by process_date when that column is present.
    """
    silver_table: str = config["silver_table"]

    if spark.catalog.tableExists(silver_table):
        _log.info("Silver table '%s' already exists — skipping creation", silver_table)
        return

    _log.info("Creating silver table '%s'", silver_table)
    schema = _schema_from_config(config["schema"])
    writer = spark.createDataFrame([], schema).write.format("delta")

    partition_col = next((f["name"] for f in config["schema"] if f["name"] == "process_date"), None)
    if partition_col:
        _log.info("Partitioning by '%s'", partition_col)
        writer = writer.partitionBy(partition_col)

    writer.saveAsTable(silver_table)
    _log.info("Silver table '%s' created successfully", silver_table)


def deduplicate(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Keep the single latest record per primary key using a window function."""
    primary_keys = config["primary_keys"]
    order_by_clauses = config["deduplication"]["order_by"]

    order_cols = [
        F.col(o["column"]).desc() if o["direction"].lower() == "desc" else F.col(o["column"]).asc()
        for o in order_by_clauses
    ]

    _log.info("Deduplicating on keys %s ordered by %s", primary_keys, [o["column"] for o in order_by_clauses])
    window_spec = Window.partitionBy(primary_keys).orderBy(*order_cols)
    return (
        df.withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def apply_soft_delete(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Remove records flagged for deletion based on YAML soft_delete config."""
    sd_config = config.get("soft_delete", {})
    if not sd_config.get("enabled", False):
        _log.info("Soft delete disabled — skipping")
        return df

    col_name: str = sd_config["column"]

    if col_name not in df.columns:
        _log.warning("Soft delete column '%s' not found in DataFrame — skipping", col_name)
        return df

    delete_value = sd_config["value"]
    _log.info("Filtering out records where %s = %s", col_name, delete_value)
    return df.filter(F.col(col_name) != F.lit(delete_value))


@with_retry(max_retries=3, delay_seconds=5)
def upsert_to_silver(
    spark: SparkSession,
    df: DataFrame,
    config: Dict[str, Any],
) -> None:
    """MERGE incoming records into the silver Delta table.

    Assumes ensure_silver_table has already been called so the table exists.
    """
    from delta.tables import DeltaTable

    silver_table: str = config["silver_table"]
    primary_keys = config["primary_keys"]

    merge_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in primary_keys]
    )
    update_set = {col: f"source.{col}" for col in df.columns}
    insert_values = {col: f"source.{col}" for col in df.columns}

    _log.info("MERGE INTO '%s' on keys %s", silver_table, primary_keys)
    (
        DeltaTable.forName(spark, silver_table)
        .alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
