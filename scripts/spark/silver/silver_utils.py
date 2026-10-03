from collections.abc import Iterable

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    current_timestamp,
    lit,
    nullif,
    regexp_replace,
    row_number,
    sha2,
    struct,
    to_json,
    trim,
    when,
)


def normalize_text(column: Column) -> Column:
    return trim(regexp_replace(column, r"\s+", " "))


def stable_hash(columns: Iterable[str]) -> Column:
    """Hash a named business-column set while retaining null positions/types."""
    return sha2(to_json(struct(*[col(c).alias(c) for c in columns])), 256)


def add_dq(
    df: DataFrame,
    *,
    key_name: str,
    key_columns: list[str],
    hash_columns: list[str],
    required_columns: list[str] | dict[str, str],
    range_conditions: list[tuple[Column, str]] | None = None,
    business_conditions: list[tuple[Column, str]] | None = None,
    warning_conditions: list[tuple[Column, str]] | None = None,
    duplicate_error_code: str = "DUPLICATE_KEY",
    dedup_order_by: list[Column] | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Add key/hash/DQ metadata and split valid and rejected DataFrames."""
    range_conditions = range_conditions or []
    business_conditions = business_conditions or []
    warning_conditions = warning_conditions or []

    if isinstance(required_columns, list):
        null_rules: dict[str, str] = {
            c: f"{c.upper()}_NULL" for c in required_columns
        }
    else:
        null_rules = required_columns

    if dedup_order_by is not None:
        dedup_window = Window.partitionBy(*key_columns).orderBy(*dedup_order_by)
        df = (
            df.withColumn("_dedup_rank", row_number().over(dedup_window))
            .where(col("_dedup_rank") == 1)
            .drop("_dedup_rank")
        )

    dq_error_expressions: list[Column] = []

    for col_name, err_code in null_rules.items():
        dq_error_expressions.append(
            when(col(col_name).isNull(), lit(err_code))
        )

    for condition, err_code in range_conditions:
        dq_error_expressions.append(when(condition, lit(err_code)))

    for condition, err_code in business_conditions:
        dq_error_expressions.append(when(condition, lit(err_code)))

    dq_warning_expressions: list[Column] = []
    for condition, warn_code in warning_conditions:
        dq_warning_expressions.append(when(condition, lit(warn_code)))

    if dq_warning_expressions:
        warning_col = nullif(
            concat_ws("|", *dq_warning_expressions), lit("")
        )
    else:
        warning_col = lit(None).cast("string")

    prepared = (
        df.withColumn(key_name, stable_hash(key_columns))
        .withColumn("row_hash", stable_hash(hash_columns))
    )

    if dedup_order_by is None:
        has_null_key = lit(False)
        for column in key_columns:
            has_null_key = has_null_key | col(column).isNull()

        dq_error_expressions.append(
            when(col("_dq_duplicate"), lit(duplicate_error_code))
        )

        prepared = (
            prepared
            .withColumn(
                "_dq_duplicate_count",
                count(lit(1)).over(Window.partitionBy(*key_columns)),
            )
            .withColumn("_dq_has_null_key", has_null_key)
            .withColumn(
                "_dq_duplicate",
                (~col("_dq_has_null_key")) & (col("_dq_duplicate_count") > 1),
            )
        )

    prepared = (
        prepared
        .withColumn(
            "dq_error_code",
            nullif(concat_ws("|", *dq_error_expressions), lit("")),
        )
        .withColumn("dq_warning_code", warning_col)
        .withColumn("dq_is_valid", col("dq_error_code").isNull())
        .withColumn("dq_checked_at", current_timestamp())
    )

    if dedup_order_by is None:
        prepared = prepared.drop(
            "_dq_duplicate_count", "_dq_has_null_key", "_dq_duplicate"
        )

    valid_df = prepared.where("dq_is_valid").drop(
        "bronze_key", key_name, "dq_error_code", "dq_is_valid"
    )
    reject_df = prepared.where("NOT dq_is_valid")

    return valid_df, reject_df
