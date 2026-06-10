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


def enforce_schema(
    df: DataFrame,
    schema: List[Dict[str, Any]],
    add_missing_columns: bool = False,
) -> DataFrame:
    """Cast columns to expected types and align the DataFrame to the declared schema.

    add_missing_columns controls what happens when a declared column is absent
    from the source DataFrame:
      True  (default) — adds the column as a null literal cast to the declared type.
      False           — logs a WARNING and skips the column; it will not appear in
                        the output. Downstream stages receive only the columns that
                        were actually present in the source.
    """
    existing_cols = set(df.columns)
    output_cols: List[str] = []

    for field in schema:
        col_name: str = field["name"]
        type_str: str = field["type"].lower()
        col_type = _TYPE_MAP.get(type_str, StringType())

        if type_str not in _TYPE_MAP:
            _log.warning(
                "Unrecognized type '%s' for column '%s' — defaulting to StringType",
                type_str, col_name,
            )

        if col_name in existing_cols:
            _log.info("Casting column '%s' to %s", col_name, col_type)
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
            output_cols.append(col_name)
        elif add_missing_columns:
            _log.info("Adding missing column '%s' as null (%s)", col_name, col_type)
            df = df.withColumn(col_name, F.lit(None).cast(col_type))
            output_cols.append(col_name)
        else:
            _log.warning(
                "Column '%s' not found in source — skipping "
                "(schema_enforcement.add_missing_columns = false)",
                col_name,
            )

    return df.select(output_cols)
