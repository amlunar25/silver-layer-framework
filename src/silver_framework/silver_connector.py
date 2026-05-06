from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def deduplicate(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    """Keep the single latest record per primary key using a window function."""
    primary_keys = config["primary_keys"]
    order_by_clauses = config["deduplication"]["order_by"]

    order_cols = [
        F.col(o["column"]).desc() if o["direction"].lower() == "desc" else F.col(o["column"]).asc()
        for o in order_by_clauses
    ]

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
        return df

    col_name: str = sd_config["column"]
    delete_value = sd_config["value"]
    return df.filter(F.col(col_name) != F.lit(delete_value))


def upsert_to_silver(
    spark: SparkSession,
    df: DataFrame,
    config: Dict[str, Any],
) -> None:
    """MERGE incoming records into the silver Delta table."""
    from delta.tables import DeltaTable

    silver_table: str = config["silver_table"]
    primary_keys = config["primary_keys"]

    merge_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in primary_keys]
    )
    update_set = {col: f"source.{col}" for col in df.columns}
    insert_values = {col: f"source.{col}" for col in df.columns}

    if DeltaTable.isDeltaTable(spark, silver_table):
        (
            DeltaTable.forName(spark, silver_table)
            .alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )
    else:
        df.write.format("delta").saveAsTable(silver_table)
