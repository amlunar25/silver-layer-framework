"""
Local simulation of the Silver Layer pipeline.

Creates a sample Bronze Delta table, runs the full pipeline, and prints
the Silver output and quarantined records.
"""

from datetime import date, datetime

from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from silver_framework.audit_logger import log_audit
from silver_framework.bronze_connector import read_bronze
from silver_framework.config_loader import load_config
from silver_framework.dq_framework import apply_dq_checks
from silver_framework.schema_enforcement import enforce_schema
from silver_framework.silver_connector import apply_soft_delete, deduplicate, upsert_to_silver
from silver_framework.transformations import apply_transformations

WAREHOUSE_DIR = "/tmp/silver-framework-warehouse"
CONFIG_PATH = "configs/entities/customer.yaml"


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("SilverFramework-Local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def seed_bronze_table(spark: SparkSession) -> None:
    """Create and populate the bronze.customers Delta table."""
    schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("Email", StringType()),        # camelCase — tests column normalization
        StructField("name", StringType()),
        StructField("phone", StringType()),
        StructField("updated_at", TimestampType()),
        StructField("process_date", DateType()),
        StructField("is_deleted", BooleanType()),
    ])

    data = [
        # customer 1: two rows — latest should win
        (1, " valid@email.com ", "Alice",   "123-456-7890", datetime(2024, 1, 10), date(2024, 1, 10), False),
        (1, "old@email.com",     "Alice",   "123-456-7890", datetime(2024, 1,  5), date(2024, 1,  5), False),
        # customer 2: invalid email — goes to quarantine
        (2, "invalid_email",     "Bob",     None,           datetime(2024, 1, 10), date(2024, 1, 10), False),
        # customer 3: soft-deleted — excluded from silver
        (3, "test@test.com",     "Charlie", "789-012-3456", datetime(2024, 1, 10), date(2024, 1, 10), True),
    ]

    bronze_path = f"{WAREHOUSE_DIR}/bronze_customers"
    spark.createDataFrame(data, schema=schema).write.format("delta").mode("overwrite").save(bronze_path)

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql(f"DROP TABLE IF EXISTS bronze.customers")
    spark.sql(f"CREATE TABLE bronze.customers USING DELTA LOCATION '{bronze_path}'")

    spark.sql("CREATE DATABASE IF NOT EXISTS silver")


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    seed_bronze_table(spark)
    config = load_config(CONFIG_PATH)

    # ── Pipeline ──────────────────────────────────────────────────────────────
    raw_df = read_bronze(spark, config, full_scan=True)

    transformed_df = apply_transformations(raw_df)

    valid_df, invalid_df = apply_dq_checks(
        transformed_df, config["dq_checks"], config["primary_keys"]
    )

    enforced_df = enforce_schema(valid_df, config["schema"])
    deduped_df = deduplicate(enforced_df, config)
    final_df = apply_soft_delete(deduped_df, config)

    upsert_to_silver(spark, final_df, config)

    log_audit(
        spark,
        config,
        input_count=raw_df.count(),
        valid_count=valid_df.count(),
        invalid_count=invalid_df.count(),
        status="SUCCESS",
    )

    # ── Results ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  SILVER OUTPUT")
    print("=" * 60)
    final_df.show(truncate=False)

    print("=" * 60)
    print("  QUARANTINE OUTPUT")
    print("=" * 60)
    invalid_df.show(truncate=False)


if __name__ == "__main__":
    main()
