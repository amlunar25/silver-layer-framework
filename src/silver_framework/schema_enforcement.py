from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from silver_framework.logger import get_logger

_log = get_logger("schema_enforcement")

_TYPE_MAP = {
    "string": StringType(),
    "integer": IntegerType(),
    "int": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "timestamp": TimestampType(),
    "date": DateType(),
}


def enforce_schema(df: DataFrame, schema: List[Dict[str, Any]]) -> DataFrame:
    """
    Cast existing columns to expected types, add missing columns as null,
    and drop any columns not in the schema.
    """
    existing_cols = set(df.columns)

    for field in schema:
        col_name: str = field["name"]
        type_str: str = field["type"].lower()
        col_type = _TYPE_MAP.get(type_str, StringType())

        if type_str not in _TYPE_MAP:
            _log.warning("Unrecognized type '%s' for column '%s' — defaulting to StringType", type_str, col_name)

        if col_name in existing_cols:
            _log.info("Casting column '%s' to %s", col_name, col_type)
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
        else:
            _log.info("Adding missing column '%s' as null (%s)", col_name, col_type)
            df = df.withColumn(col_name, F.lit(None).cast(col_type))

    expected_cols = [f["name"] for f in schema]
    return df.select(expected_cols)
