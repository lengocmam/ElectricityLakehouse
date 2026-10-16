from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from gold.gold_utils import build_date_key


def transform_fact_hydro_status(
    hydro_df: DataFrame,
    dim_plant_df: DataFrame,
) -> DataFrame:
    """Transform silver hydro reservoir data into fact_hydro_status."""
    # Parse observation hour from string format "HH:mm" (e.g., "23:00")
    obs_time_col = F.when(
        F.col("observation_time").isNotNull() & F.col("observation_time").rlike(r"^\d{1,2}:\d{2}"),
        F.split(F.col("observation_time"), ":").getItem(0).cast("int"),
    ).otherwise(F.lit(23))

    h_df = hydro_df.alias("h")
    p_df = dim_plant_df.alias("p")

    enriched = (
        h_df
        .join(
            F.broadcast(p_df),
            F.trim(F.lower(F.col("h.plant_name"))) == F.trim(F.lower(F.col("p.plant_name"))),
            "inner",
        )
        .withColumn("date_key", build_date_key(F.col("h.data_date")))
        .withColumn("observation_time_key", obs_time_col.cast("int"))
    )

    # Deterministic surrogate key: parallel O(1), eliminates single-partition shuffle bottleneck
    surrogate_key = F.abs(F.hash(F.col("date_key"), F.col("observation_time_key"), F.col("plant_key"))).cast("int")

    result = (
        enriched
        .withColumn("hydro_key", surrogate_key)
        .withColumn("upstream_water_level_m", F.col("upstream_water_level_m").cast("float"))
        .withColumn("normal_water_level_m", F.col("normal_water_level_m").cast("float"))
        .withColumn("dead_water_level_m", F.col("dead_water_level_m").cast("float"))
        .withColumn("reservoir_inflow_m3_per_s", F.col("reservoir_inflow_m3_per_s").cast("float"))
        .withColumn("total_discharge_m3_per_s", F.col("total_discharge_m3_per_s").cast("float"))
        .withColumn("spillway_discharge_m3_per_s", F.col("spillway_discharge_m3_per_s").cast("float"))
        .withColumn("powerhouse_discharge_m3_per_s", F.col("powerhouse_discharge_m3_per_s").cast("float"))
        .withColumn("deep_discharge_gate_count", F.col("deep_discharge_gate_count").cast("int"))
        .withColumn("surface_discharge_gate_count", F.col("surface_discharge_gate_count").cast("int"))
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
