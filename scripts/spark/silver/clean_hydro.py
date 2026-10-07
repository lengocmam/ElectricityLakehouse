from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, regexp_replace

from silver.silver_utils import add_dq, normalize_text


def clean_hydro(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Execute clean hydro logic."""
    prepared = (
        df.withColumn(
            "plant_name",
            normalize_text(regexp_replace("plant_name", r"(?i)\s+\u0111\u1ed3ng\s+b\u1ed9\s+l\u00fac:.*$", "")),
        )
        .withColumn("spillway_discharge_m3_per_s", coalesce(col("spillway_discharge_m3_per_s"), lit(0.0)))
        .withColumn("powerhouse_discharge_m3_per_s", coalesce(col("powerhouse_discharge_m3_per_s"), lit(0.0)))
        .withColumn("deep_discharge_gate_count", coalesce(col("deep_discharge_gate_count"), lit(0)))
        .withColumn("surface_discharge_gate_count", coalesce(col("surface_discharge_gate_count"), lit(0)))
    )

    range_conditions = [
        (col("upstream_water_level_m").isNotNull() & (col("upstream_water_level_m") < 0), "WATER_LEVEL_NEGATIVE"),
        (col("normal_water_level_m").isNotNull() & (col("normal_water_level_m") < 0), "NORMAL_LEVEL_NEGATIVE"),
        (col("dead_water_level_m").isNotNull() & (col("dead_water_level_m") < 0), "DEAD_LEVEL_NEGATIVE"),
        (col("reservoir_inflow_m3_per_s").isNotNull() & (col("reservoir_inflow_m3_per_s") < 0), "INFLOW_NEGATIVE"),
        (col("total_discharge_m3_per_s").isNotNull() & (col("total_discharge_m3_per_s") < 0), "TOTAL_OUTFLOW_NEGATIVE"),
        (col("spillway_discharge_m3_per_s").isNotNull() & (col("spillway_discharge_m3_per_s") < 0), "SPILLWAY_OUTFLOW_NEGATIVE"),
        (col("powerhouse_discharge_m3_per_s").isNotNull() & (col("powerhouse_discharge_m3_per_s") < 0), "POWERHOUSE_OUTFLOW_NEGATIVE"),
        (col("deep_discharge_gate_count").isNotNull() & (col("deep_discharge_gate_count") < 0), "DEEP_OUTLET_GATE_COUNT_NEGATIVE"),
        (col("surface_discharge_gate_count").isNotNull() & (col("surface_discharge_gate_count") < 0), "SURFACE_OUTLET_GATE_COUNT_NEGATIVE"),
    ]

    warning_conditions = [
        (
            col("upstream_water_level_m").isNotNull()
            & col("normal_water_level_m").isNotNull()
            & (col("upstream_water_level_m") > col("normal_water_level_m")),
            "WATER_LEVEL_ABOVE_NORMAL",
        ),
        (
            col("upstream_water_level_m").isNotNull()
            & col("dead_water_level_m").isNotNull()
            & (col("upstream_water_level_m") < col("dead_water_level_m")),
            "WATER_LEVEL_BELOW_DEAD",
        ),
    ]

    return add_dq(
        prepared, key_name="evn_hydro_key", key_columns=["data_date", "plant_name"],
        hash_columns=[
            "data_date", "plant_name", "observation_time",
            "upstream_water_level_m", "normal_water_level_m", "dead_water_level_m",
            "reservoir_inflow_m3_per_s", "total_discharge_m3_per_s",
            "spillway_discharge_m3_per_s", "powerhouse_discharge_m3_per_s",
            "deep_discharge_gate_count", "surface_discharge_gate_count",
        ],
        required_columns={
            "bronze_key": "BRONZE_KEY_NULL",
            "data_date": "DATA_DATE_NULL",
            "plant_name": "RESERVOIR_NAME_NULL",
            "observation_time": "OBS_TIME_NULL",
            "upstream_water_level_m": "WATER_LEVEL_NULL",
            "normal_water_level_m": "NORMAL_LEVEL_NULL",
            "dead_water_level_m": "DEAD_LEVEL_NULL",
            "reservoir_inflow_m3_per_s": "INFLOW_NULL",
            "total_discharge_m3_per_s": "TOTAL_OUTFLOW_NULL",
            "spillway_discharge_m3_per_s": "SPILLWAY_OUTFLOW_NULL",
            "powerhouse_discharge_m3_per_s": "POWERHOUSE_OUTFLOW_NULL",
            "deep_discharge_gate_count": "DEEP_OUTLET_GATE_COUNT_NULL",
            "surface_discharge_gate_count": "SURFACE_OUTLET_GATE_COUNT_NULL",
        },
        range_conditions=range_conditions,
        warning_conditions=warning_conditions,
        dedup_order_by=[col("bronze_key").desc()],
    )