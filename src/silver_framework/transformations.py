import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Rename all columns to snake_case."""
    for col in df.columns:
        snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", col).lower()
        snake = re.sub(r"[^a-z0-9_]", "_", snake)
        snake = re.sub(r"_+", "_", snake).strip("_")
        if col != snake:
            df = df.withColumnRenamed(col, snake)
    return df


def trim_strings(df: DataFrame) -> DataFrame:
    """Strip leading/trailing whitespace from all string columns."""
    string_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))
    return df


def apply_transformations(df: DataFrame) -> DataFrame:
    df = normalize_column_names(df)
    df = trim_strings(df)
    return df
