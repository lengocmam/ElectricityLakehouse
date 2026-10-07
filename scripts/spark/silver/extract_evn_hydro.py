from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import (
    element_at,
    explode,
    lit,
    nullif,
    regexp_extract_all,
    regexp_replace,
    size,
    to_date,
    transform,
    trim,
)


def _html_text(column):
    """Execute html text logic."""
    return trim(regexp_replace(regexp_replace(regexp_replace(column, "(?is)<[^>]+>", " "), "(?i)&nbsp;|&#160;", " "), "\\s+", " "))


def _nullable_decimal(column):
    """Cast an optional Hydro numeric cell after source-format normalisation."""
    normalized = regexp_replace(trim(column), r"[\s\u00a0]", "")
    return nullif(regexp_replace(normalized, ",", "."), lit("")).cast("double")


def _nullable_int(column):
    """Execute nullable int logic."""
    return nullif(trim(column), lit("")).cast("int")


def extract_evn_hydro(df: DataFrame) -> DataFrame: 
    """Return one row per plant and source snapshot day, with all nine raw measures.""" 
    rows = regexp_extract_all("raw", lit("(?is)<tr[^>]*>(.*?)</tr>"), 1) 
    cells = transform(rows, lambda row: transform(regexp_extract_all(row, lit("(?is)<td[^>]*>(.*?)</td>"), 1), _html_text)) 
    plant = explode(F.filter(cells, lambda row: size(row) == lit(11))) 
    return (df.select("bronze_key", "source_data_date", plant.alias("plant_row")) 
        .select( 
            "bronze_key", 
            to_date("source_data_date").alias("data_date"), 
            element_at("plant_row", 1).alias("plant_name"), 
            element_at("plant_row", 2).alias("observation_time"),
            _nullable_decimal(element_at("plant_row", 3)).alias("upstream_water_level_m"), 
            _nullable_decimal(element_at("plant_row", 4)).alias("normal_water_level_m"), 
            _nullable_decimal(element_at("plant_row", 5)).alias("dead_water_level_m"), 
            _nullable_decimal(element_at("plant_row", 6)).alias("reservoir_inflow_m3_per_s"), 
            _nullable_decimal(element_at("plant_row", 7)).alias("total_discharge_m3_per_s"), 
            _nullable_decimal(element_at("plant_row", 8)).alias("spillway_discharge_m3_per_s"), 
            _nullable_decimal(element_at("plant_row", 9)).alias("powerhouse_discharge_m3_per_s"), 
            _nullable_int(element_at("plant_row", 10)).alias("deep_discharge_gate_count"), 
            _nullable_int(element_at("plant_row", 11)).alias("surface_discharge_gate_count"), 
        )
    ) 
