from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import (
    array,
    element_at,
    explode,
    lit,
    lower,
    nullif,
    regexp_extract,
    regexp_extract_all,
    regexp_replace,
    size,
    struct,
    to_date,
    transform,
    trim,
)


def _html_text(column):
    """Execute html text logic."""
    return trim(regexp_replace(regexp_replace(regexp_replace(column, "(?is)<[^>]+>", " "), "(?i)&nbsp;|&#160;", " "), "\\s+", " "))


def _decimal(column):
    """Execute decimal logic."""
    normalized = regexp_replace(trim(column), r"\s+", "")
    normalized = regexp_replace(normalized, ",", ".")
    is_number = normalized.rlike(r"^[-+]?[0-9]*\.?[0-9]+$")
    return F.when(is_number, normalized.cast("double")).otherwise(F.lit(None).cast("double"))


def extract_evn_power(df: DataFrame) -> DataFrame:
    """Return the national dispatched power by date and requested load period."""
    power_table = regexp_extract(
        "raw",
        "(?is)C\u00d4NG\\s+SU\u1ea4T\\s+HUY\\s+\u0110\u1ed8NG.*?</table>",
        0,
    )
    rows = regexp_extract_all(power_table, lit("(?is)<tr[^>]*>(.*?)</tr>"), 1)
    cells = transform(rows, lambda row: transform(regexp_extract_all(row, lit("(?is)<t[dh][^>]*>(.*?)</t[dh]>"), 1), _html_text))
    rows_with_power_values = F.filter(cells, lambda row: size(row) >= lit(3))
    data_rows = F.filter(
        rows_with_power_values,
        lambda row: lower(element_at(row, 1)) != lit("ngu\u1ed3n")
    )

    return (df.select("bronze_key", "source_data_date", explode(data_rows).alias("power_row"))
        .select("bronze_key", "source_data_date", explode(array(
            struct(
                element_at("power_row", 1).alias("generation_source"),
                lit("midday_off_peak").alias("period"), 
                _decimal(element_at("power_row", 2)).alias("dispatched_power_mw")
            ),
            struct(
                element_at("power_row", 1).alias("generation_source"),
                lit("evening_peak").alias("period"), 
                _decimal(element_at("power_row", 3)).alias("dispatched_power_mw")
            ),
        )).alias("power"))
        .select("bronze_key", "source_data_date", "power.*")
        .withColumn("data_date", to_date("source_data_date"))
        .select("bronze_key", "data_date", "generation_source", "period", "dispatched_power_mw"))


def extract_evn_generation(df: DataFrame) -> DataFrame:
    """Return one daily generation record for each raw EVN energy source."""
    lines = transform(regexp_extract_all("raw", lit("(?is)<p>\\s*-\\s*(.*?)</p>"), 1), _html_text)
    return (df.select("bronze_key", "source_data_date", explode(lines).alias("generation_line"))
        .withColumn("generation_line", regexp_replace("generation_line", "triệu", "triệu"))
        .withColumn("energy_source", trim(regexp_extract("generation_line", "^(.*?)(?:\\s+[-+]?[0-9])", 1)))
        .withColumn("generation_million_kwh", _decimal(regexp_extract("generation_line", "([-+]?[0-9]+(?:,[0-9]+)?)\\s*triệu\\s*kWh", 1)))
        .withColumn("data_date", to_date("source_data_date"))
        .select("bronze_key", "data_date", "energy_source", "generation_million_kwh"))

