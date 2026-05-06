"""
Local simulation of the Silver Layer pipeline — multi-entity parallel run.

Seeds bronze.customers and bronze.orders Delta tables, then executes both
pipelines concurrently via run_entities_parallel and prints per-entity results.
"""

import shutil
from datetime import date, datetime

from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from silver_framework.audit_logger import _AUDIT_SCHEMA
from silver_framework.pipeline_runner import run_entities_parallel

WAREHOUSE_DIR = "/tmp/silver-framework-warehouse"
CONFIG_CUSTOMERS = "configs/entities/customer.yaml"
CONFIG_ORDERS = "configs/entities/orders.yaml"


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("SilverFramework-Local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        # Large-scale tuning: limit shuffle partitions for local runs;
        # set to 200+ in production
        .config("spark.sql.shuffle.partitions", "8")
        # 128 MB target partition size during file scan
        .config("spark.sql.files.maxPartitionBytes", "134217728")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def seed_bronze_customers(spark: SparkSession) -> None:
    schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("Email", StringType()),        # camelCase — tests normalization
        StructField("name", StringType()),
        StructField("phone", StringType()),
        StructField("updated_at", TimestampType()),
        StructField("process_date", DateType()),
        StructField("is_deleted", BooleanType()),
    ])
    data = [
        (1, " valid@email.com ", "Alice",   "123-456-7890", datetime(2024, 1, 10), date(2024, 1, 10), False),
        (1, "old@email.com",     "Alice",   "123-456-7890", datetime(2024, 1,  5), date(2024, 1,  5), False),
        (2, "invalid_email",     "Bob",     None,           datetime(2024, 1, 10), date(2024, 1, 10), False),
        (3, "test@test.com",     "Charlie", "789-012-3456", datetime(2024, 1, 10), date(2024, 1, 10), True),
    ]
    path = f"{WAREHOUSE_DIR}/bronze_customers"
    spark.createDataFrame(data, schema=schema).write.format("delta").mode("overwrite").save(path)
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("DROP TABLE IF EXISTS bronze.customers")
    spark.sql(f"CREATE TABLE bronze.customers USING DELTA LOCATION '{path}'")


def _ensure_audit_table(spark: SparkSession) -> None:
    """Create an empty audit Delta table so parallel entities can append safely."""
    from delta.tables import DeltaTable
    audit_table = "silver.audit_log"
    if not DeltaTable.isDeltaTable(spark, audit_table):
        spark.createDataFrame([], _AUDIT_SCHEMA).write.format("delta").saveAsTable(audit_table)


def seed_bronze_orders(spark: SparkSession) -> None:
    schema = StructType([
        StructField("order_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("amount", DoubleType()),
        StructField("status", StringType()),
        StructField("order_date", TimestampType()),
        StructField("process_date", DateType()),
        StructField("is_deleted", BooleanType()),
    ])
    data = [
        # order 10: duplicate rows — latest wins
        (10, 1, 99.99,  "shipped", datetime(2024, 1, 10), date(2024, 1, 10), False),
        (10, 1, 99.99,  "pending", datetime(2024, 1,  5), date(2024, 1,  5), False),
        # order 20: null amount — DQ failure → quarantine
        (20, 2, None,   "pending", datetime(2024, 1, 10), date(2024, 1, 10), False),
        # order 30: soft-deleted
        (30, 3, 49.00,  "shipped", datetime(2024, 1, 10), date(2024, 1, 10), True),
    ]
    path = f"{WAREHOUSE_DIR}/bronze_orders"
    spark.createDataFrame(data, schema=schema).write.format("delta").mode("overwrite").save(path)
    spark.sql("DROP TABLE IF EXISTS bronze.orders")
    spark.sql(f"CREATE TABLE bronze.orders USING DELTA LOCATION '{path}'")


def main() -> None:
    # Clean previous run artifacts so the fresh metastore finds no stale files
    shutil.rmtree(WAREHOUSE_DIR, ignore_errors=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    seed_bronze_customers(spark)
    seed_bronze_orders(spark)

    # Pre-create the shared audit table before the parallel run to avoid a
    # race condition where two entities both try to create it simultaneously.
    _ensure_audit_table(spark)

    results = run_entities_parallel(
        spark,
        config_paths=[CONFIG_CUSTOMERS, CONFIG_ORDERS],
        full_scan=True,
        max_workers=2,
    )

    print("\n" + "=" * 60)
    print("  PIPELINE SUMMARY")
    print("=" * 60)
    for r in results:
        status = r["status"]
        print(
            f"  {r['entity']:<20} {status:<8} "
            f"input={r['input_count']}  valid={r['valid_count']}  "
            f"quarantine={r['invalid_count']}"
            + (f"  error={r['error']}" if r["error"] else "")
        )
    print("=" * 60)

    succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
    print(f"\n  {succeeded}/{len(results)} entities succeeded\n")


if __name__ == "__main__":
    main()
