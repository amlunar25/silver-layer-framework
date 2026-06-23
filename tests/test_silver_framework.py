"""
Unit tests for Silver Layer Framework modules.
Tests DQ framework, schema enforcement, deduplication, soft delete,
and transformations using a local SparkSession.
"""

from datetime import date, datetime
from unittest.mock import MagicMock

import pytest
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

from silver_framework.dq_framework import apply_dq_checks
from silver_framework.retry import with_retry
from silver_framework.schema_enforcement import enforce_schema
from silver_framework.silver_connector import apply_soft_delete, deduplicate
from silver_framework.transformations import apply_transformations


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName("TestSilverFramework")
        .getOrCreate()
    )


# ── DQ Framework ──────────────────────────────────────────────────────────────

class TestDQFramework:
    def test_not_null_splits_valid_and_invalid(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("email", StringType()),
        ])
        df = spark.createDataFrame([(1, "a@b.com"), (2, None)], schema=schema)

        valid, invalid = apply_dq_checks(df, [{"column": "email", "type": "not_null"}], ["id"])

        assert valid.count() == 1
        assert invalid.count() == 1
        assert valid.collect()[0]["id"] == 1

    def test_regex_splits_valid_and_invalid(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("email", StringType()),
        ])
        df = spark.createDataFrame(
            [(1, "valid@email.com"), (2, "invalid_email")], schema=schema
        )
        checks = [{
            "column": "email",
            "type": "regex",
            "pattern": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
        }]

        valid, invalid = apply_dq_checks(df, checks, ["id"])

        assert valid.count() == 1
        assert invalid.count() == 1

    def test_multiple_checks_union_failures(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("email", StringType()),
        ])
        # id=1: null email (not_null fail)
        # id=2: invalid format (regex fail)
        # id=3: valid
        df = spark.createDataFrame(
            [(1, None), (2, "bad"), (3, "ok@ok.com")], schema=schema
        )
        checks = [
            {"column": "email", "type": "not_null"},
            {"column": "email", "type": "regex", "pattern": r"^[^@]+@[^@]+\.[^@]+$"},
        ]

        valid, invalid = apply_dq_checks(df, checks, ["id"])

        assert valid.count() == 1
        assert valid.collect()[0]["id"] == 3

    def test_empty_checks_returns_all_valid(self, spark: SparkSession) -> None:
        schema = StructType([StructField("id", IntegerType())])
        df = spark.createDataFrame([(1,), (2,)], schema=schema)

        valid, invalid = apply_dq_checks(df, [], ["id"])

        assert valid.count() == 2
        assert invalid.count() == 0

    def test_invalid_df_contains_failed_check_column(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
        ])
        df = spark.createDataFrame([(1, None)], schema=schema)

        _, invalid = apply_dq_checks(df, [{"column": "name", "type": "not_null"}], ["id"])

        assert "_dq_failed_check" in invalid.columns
        assert "not_null:name" in invalid.collect()[0]["_dq_failed_check"]


# ── Schema Enforcement ────────────────────────────────────────────────────────

class TestSchemaEnforcement:
    def test_cast_string_to_integer(self, spark: SparkSession) -> None:
        schema = StructType([StructField("id", StringType())])
        df = spark.createDataFrame([("42",)], schema=schema)

        result = enforce_schema(df, [{"name": "id", "type": "integer"}])

        assert dict(result.dtypes)["id"] == "int"
        assert result.collect()[0]["id"] == 42

    def test_adds_missing_column_as_null(self, spark: SparkSession) -> None:
        schema = StructType([StructField("id", IntegerType())])
        df = spark.createDataFrame([(1,)], schema=schema)
        target = [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
        ]

        result = enforce_schema(df, target, add_missing_columns=True)

        assert "name" in result.columns
        assert result.collect()[0]["name"] is None

    def test_drops_extra_columns(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("extra", StringType()),
        ])
        df = spark.createDataFrame([(1, "x")], schema=schema)

        result = enforce_schema(df, [{"name": "id", "type": "integer"}])

        assert result.columns == ["id"]

    def test_preserves_column_order(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("z", IntegerType()),
            StructField("a", IntegerType()),
        ])
        df = spark.createDataFrame([(1, 2)], schema=schema)
        target = [{"name": "a", "type": "integer"}, {"name": "z", "type": "integer"}]

        result = enforce_schema(df, target)

        assert result.columns == ["a", "z"]


# ── Deduplication ─────────────────────────────────────────────────────────────

class TestDeduplication:
    def _config(self) -> dict:
        return {
            "primary_keys": ["customer_id"],
            "deduplication": {
                "order_by": [
                    {"column": "updated_at", "direction": "desc"},
                    {"column": "process_date", "direction": "desc"},
                ]
            },
        }

    def test_keeps_latest_record_per_key(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("customer_id", IntegerType()),
            StructField("updated_at", TimestampType()),
            StructField("process_date", DateType()),
        ])
        data = [
            (1, datetime(2024, 1, 10), date(2024, 1, 10)),
            (1, datetime(2024, 1,  5), date(2024, 1,  5)),
            (2, datetime(2024, 1, 10), date(2024, 1, 10)),
        ]
        df = spark.createDataFrame(data, schema=schema)

        result = deduplicate(df, self._config())

        assert result.count() == 2
        row = result.filter("customer_id = 1").collect()[0]
        assert row["updated_at"] == datetime(2024, 1, 10)

    def test_no_row_num_column_in_output(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("customer_id", IntegerType()),
            StructField("updated_at", TimestampType()),
            StructField("process_date", DateType()),
        ])
        df = spark.createDataFrame(
            [(1, datetime(2024, 1, 1), date(2024, 1, 1))], schema=schema
        )

        result = deduplicate(df, self._config())

        assert "_row_num" not in result.columns

    def test_single_record_per_key_unchanged(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("customer_id", IntegerType()),
            StructField("updated_at", TimestampType()),
            StructField("process_date", DateType()),
        ])
        df = spark.createDataFrame(
            [(1, datetime(2024, 1, 1), date(2024, 1, 1))], schema=schema
        )

        result = deduplicate(df, self._config())

        assert result.count() == 1


# ── Soft Delete ───────────────────────────────────────────────────────────────

class TestSoftDelete:
    def test_removes_deleted_records(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("is_deleted", BooleanType()),
        ])
        df = spark.createDataFrame([(1, False), (2, True), (3, False)], schema=schema)
        config = {"soft_delete": {"enabled": True, "column": "is_deleted", "value": True}}

        result = apply_soft_delete(df, config)

        assert result.count() == 2
        ids = {row["id"] for row in result.collect()}
        assert 2 not in ids

    def test_disabled_returns_all_records(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("is_deleted", BooleanType()),
        ])
        df = spark.createDataFrame([(1, False), (2, True)], schema=schema)
        config = {"soft_delete": {"enabled": False}}

        result = apply_soft_delete(df, config)

        assert result.count() == 2

    def test_no_soft_delete_config_returns_all_records(self, spark: SparkSession) -> None:
        schema = StructType([StructField("id", IntegerType())])
        df = spark.createDataFrame([(1,), (2,)], schema=schema)

        result = apply_soft_delete(df, {})

        assert result.count() == 2


# ── Transformations ───────────────────────────────────────────────────────────

class TestTransformations:
    def test_normalizes_camel_case_columns(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("CustomerId", StringType()),
            StructField("EmailAddress", StringType()),
        ])
        df = spark.createDataFrame([("1", "a@b.com")], schema=schema)

        result = apply_transformations(df)

        assert "customer_id" in result.columns
        assert "email_address" in result.columns

    def test_trims_string_columns(self, spark: SparkSession) -> None:
        schema = StructType([StructField("name", StringType())])
        df = spark.createDataFrame([("  Alice  ",)], schema=schema)

        result = apply_transformations(df)

        assert result.collect()[0]["name"] == "Alice"

    def test_non_string_columns_untouched(self, spark: SparkSession) -> None:
        schema = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
        ])
        df = spark.createDataFrame([(42, "  Bob  ")], schema=schema)

        result = apply_transformations(df)

        assert result.collect()[0]["id"] == 42
        assert result.collect()[0]["name"] == "Bob"


# ── Retry Decorator ───────────────────────────────────────────────────────────

class TestRetry:
    def test_retries_then_succeeds(self) -> None:
        call_count = 0

        @with_retry(max_retries=3, delay_seconds=0)
        def flaky() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("transient error")
            return "ok"

        result = flaky()
        assert result == "ok"
        assert call_count == 3

    def test_raises_after_max_retries(self) -> None:
        mock_fn = MagicMock(side_effect=RuntimeError("permanent error"))

        @with_retry(max_retries=3, delay_seconds=0)
        def always_fails() -> None:
            mock_fn()

        with pytest.raises(RuntimeError, match="permanent error"):
            always_fails()

        assert mock_fn.call_count == 3

    def test_succeeds_on_first_attempt_no_retry(self) -> None:
        mock_fn = MagicMock(return_value="immediate")

        @with_retry(max_retries=3, delay_seconds=0)
        def succeeds() -> str:
            return mock_fn()

        result = succeeds()
        assert result == "immediate"
        assert mock_fn.call_count == 1

    def test_preserves_function_name(self) -> None:
        @with_retry(max_retries=2, delay_seconds=0)
        def my_function() -> None:
            pass

        assert my_function.__name__ == "my_function"
