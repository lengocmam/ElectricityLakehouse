from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from gold.gold_utils import build_date_key
from pyspark.sql.functions import (
    abs, broadcast, col, hash, lit,
    lower, split, trim, when,
)


def transform_fact_hydro_status(
    hydro_df: DataFrame,
    dim_plant_df: DataFrame,
) -> DataFrame:
    """Transform silver hydro reservoir data into fact_hydro_status."""
    # Parse observation hour from string format "HH:mm" (e.g., "23:00")
    obs_time_col = when(
        col("observation_time").isNotNull() & col("observation_time").rlike(r"^\d{1,2}:\d{2}"),
        split(col("observation_time"), ":").getItem(0).cast("int"),
    ).otherwise(lit(23))

    h_df = hydro_df.alias("h")
    p_df = dim_plant_df.alias("p")

    enriched = (
        h_df
        .join(
            broadcast(p_df),
            trim(lower(col("h.plant_name"))) == trim(lower(col("p.plant_name"))),
            "inner",
        )
        .withColumn("date_key", build_date_key(col("h.data_date")))
        .withColumn("observation_time_key", obs_time_col.cast("int"))
    )

    # Deterministic surrogate key: parallel O(1), eliminates single-partition shuffle bottleneck
    surrogate_key = abs(hash(col("date_key"), col("observation_time_key"), col("plant_key"))).cast("int")

    result = (
        enriched
        .withColumn("hydro_key", surrogate_key)
        .withColumn("upstream_water_level_m", col("upstream_water_level_m").cast("float"))
        .withColumn("normal_water_level_m", col("normal_water_level_m").cast("float"))
        .withColumn("dead_water_level_m", col("dead_water_level_m").cast("float"))
        .withColumn("reservoir_inflow_m3_per_s", col("reservoir_inflow_m3_per_s").cast("float"))
        .withColumn("total_discharge_m3_per_s", col("total_discharge_m3_per_s").cast("float"))
        .withColumn("spillway_discharge_m3_per_s", col("spillway_discharge_m3_per_s").cast("float"))
        .withColumn("powerhouse_discharge_m3_per_s", col("powerhouse_discharge_m3_per_s").cast("float"))
        .withColumn("deep_discharge_gate_count", col("deep_discharge_gate_count").cast("int"))
        .withColumn("surface_discharge_gate_count", col("surface_discharge_gate_count").cast("int"))
        .select(
            "hydro_key",
            "date_key",
            "observation_time_key",
            "plant_key",
            "upstream_water_level_m",
            "normal_water_level_m",
            "dead_water_level_m",
            "reservoir_inflow_m3_per_s",
            "total_discharge_m3_per_s",
            "spillway_discharge_m3_per_s",
            "powerhouse_discharge_m3_per_s",
            "deep_discharge_gate_count",
            "surface_discharge_gate_count",
        )
    )

    return result
