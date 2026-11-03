"""Build Pre-aggregated BI Rollup Tables in Gold Layer for High-Performance Dashboards.

Generates:
1. nessie.gold.agg_dashboard1_supply_demand_hourly:
   Pre-calculated 24h supply-demand, duck curve, regional load, and renewable mix.
2. nessie.gold.agg_dashboard2_climate_hydro_daily:
   Pre-calculated reservoir useful capacity %, flood alerts, inflow vs discharge, and weather correlation.
"""
import argparse
import os
import sys

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from utils.spark import create_spark_session
from utils.logging import create_logger

logger = create_logger(__name__)


def build_dashboard1_supply_demand_rollup(spark: SparkSession) -> DataFrame:
    """Build pre-aggregated rollup for Dashboard 1 (Supply-Demand & Energy Transition)."""
    logger.info("Computing Dashboard 1 Rollup (Supply-Demand & Energy Transition)...")

    load_df = spark.table("nessie.gold.fact_load_hourly")
    date_df = spark.table("nessie.gold.dim_date")
    time_df = spark.table("nessie.gold.dim_time")
    region_df = spark.table("nessie.gold.dim_region")
    gen_df = spark.table("nessie.gold.fact_generation_daily")
    source_df = spark.table("nessie.gold.dim_energy_source")

    # 1. Hourly regional and national load
    joined_load = (
        load_df.alias("l")
        .join(date_df.alias("d"), F.col("l.date_key") == F.col("d.date_key"), "inner")
        .join(time_df.alias("t"), F.col("l.time_key") == F.col("t.time_key"), "inner")
        .join(region_df.alias("r"), F.col("l.region_key") == F.col("r.region_key"), "inner")
        .groupBy(
            "l.date_key",
            "d.full_date",
            "d.month",
            "d.season",
            "d.is_weekend",
            "t.hour",
        )
        .agg(
            F.sum("l.load_mw").alias("national_load_mw"),
            F.sum(F.when(F.lower(F.col("r.region_name")) == "north", F.col("l.load_mw")).otherwise(0)).alias("north_load_mw"),
            F.sum(F.when(F.lower(F.col("r.region_name")) == "central", F.col("l.load_mw")).otherwise(0)).alias("central_load_mw"),
            F.sum(F.when(F.lower(F.col("r.region_name")) == "south", F.col("l.load_mw")).otherwise(0)).alias("south_load_mw"),
        )
    )

    # 2. Daily generation summary
    gen_summary = (
        gen_df.alias("g")
        .join(source_df.alias("s"), F.col("g.energy_source_key") == F.col("s.energy_source_key"), "inner")
        .groupBy("g.date_key")
        .agg(
            F.sum("g.generation_mwh").alias("total_generation_mwh"),
            F.sum(F.when(F.col("s.renewable_flag") == True, F.col("g.generation_mwh")).otherwise(0)).alias("renewable_generation_mwh"),
            F.sum(F.when(F.lower(F.col("s.source_code")) == "solar", F.col("g.generation_mwh")).otherwise(0)).alias("solar_generation_mwh"),
            F.sum(F.when(F.lower(F.col("s.source_code")) == "wind", F.col("g.generation_mwh")).otherwise(0)).alias("wind_generation_mwh"),
            F.sum(F.when(F.lower(F.col("s.source_code")) == "hydro", F.col("g.generation_mwh")).otherwise(0)).alias("hydro_generation_mwh"),
            F.sum(F.when(F.lower(F.col("s.source_code")) == "coal", F.col("g.generation_mwh")).otherwise(0)).alias("coal_generation_mwh"),
            F.sum(F.when(F.lower(F.col("s.source_code")) == "gas", F.col("g.generation_mwh")).otherwise(0)).alias("gas_generation_mwh"),
        )
        .withColumn(
            "renewable_share_pct",
            F.round(F.col("renewable_generation_mwh") * 100.0 / F.when(F.col("total_generation_mwh") > 0, F.col("total_generation_mwh")).otherwise(1.0), 2)
        )
    )

    # 3. Combine and compute Duck Curve Net Load
    result = (
        joined_load.alias("jl")
        .join(gen_summary.alias("gs"), F.col("jl.date_key") == F.col("gs.date_key"), "left")
        .withColumn("avg_hourly_generation_mw", F.round(F.col("gs.total_generation_mwh") / 24.0, 1))
        .withColumn("avg_hourly_solar_mw", F.round(F.col("gs.solar_generation_mwh") / 24.0, 1))
        .withColumn("avg_hourly_wind_mw", F.round(F.col("gs.wind_generation_mwh") / 24.0, 1))
        .withColumn("avg_hourly_hydro_mw", F.round(F.col("gs.hydro_generation_mwh") / 24.0, 1))
        .withColumn("avg_hourly_coal_mw", F.round(F.col("gs.coal_generation_mwh") / 24.0, 1))
        .withColumn("avg_hourly_gas_mw", F.round(F.col("gs.gas_generation_mwh") / 24.0, 1))
        # Simulated Duck Curve Net Load profile: midday solar belly
        .withColumn(
            "solar_hourly_shape_mw",
            F.when(
                (F.col("jl.hour") >= 6) & (F.col("jl.hour") <= 18),
                (F.col("gs.solar_generation_mwh") * 1.8 / 12.0) * F.sin(F.lit(3.14159) * (F.col("jl.hour") - 6) / 12.0)
            ).otherwise(0.0)
        )
        .withColumn(
            "net_load_mw",
            F.round(F.col("jl.national_load_mw") - F.col("solar_hourly_shape_mw") - (F.col("gs.wind_generation_mwh") / 24.0), 1)
        )
        .drop("solar_hourly_shape_mw")
        .orderBy("jl.date_key", "jl.hour")
    )

    return result


def build_dashboard2_climate_hydro_rollup(spark: SparkSession) -> DataFrame:
    """Build pre-aggregated rollup for Dashboard 2 (Climate, Hydropower & Water Security)."""
    logger.info("Computing Dashboard 2 Rollup (Climate, Hydropower & Water Security)...")

    hydro_df = spark.table("nessie.gold.fact_hydro_status")
    plant_df = spark.table("nessie.gold.dim_plant")
    date_df = spark.table("nessie.gold.dim_date")
    weather_df = spark.table("nessie.gold.fact_weather_hourly")
    loc_df = spark.table("nessie.gold.dim_location")
    load_df = spark.table("nessie.gold.fact_load_hourly")
    gen_df = spark.table("nessie.gold.fact_generation_daily")
    source_df = spark.table("nessie.gold.dim_energy_source")

    # 1. Hydro status enriched with alert flags & useful capacity
    hydro_enriched = (
        hydro_df.alias("h")
        .join(plant_df.alias("p"), F.col("h.plant_key") == F.col("p.plant_key"), "inner")
        .join(date_df.alias("d"), F.col("h.date_key") == F.col("d.date_key"), "inner")
        .select(
            F.col("h.date_key"),
            F.col("d.full_date"),
            F.col("d.month"),
            F.col("d.season"),
            F.col("p.plant_key"),
            F.col("p.plant_name"),
            F.col("p.river_basin"),
            F.col("h.upstream_water_level_m"),
            F.col("h.normal_water_level_m"),
            F.col("h.dead_water_level_m"),
            F.col("h.reservoir_inflow_m3_per_s"),
            F.col("h.total_discharge_m3_per_s"),
            F.col("h.spillway_discharge_m3_per_s"),
            F.col("h.powerhouse_discharge_m3_per_s"),
            F.col("h.deep_discharge_gate_count"),
            F.col("h.surface_discharge_gate_count"),
            (F.col("h.deep_discharge_gate_count") + F.col("h.surface_discharge_gate_count")).alias("total_gates_open"),
        )
        .withColumn(
            "useful_capacity_pct",
            F.round(
                (F.col("upstream_water_level_m") - F.col("dead_water_level_m")) * 100.0 /
                F.when((F.col("normal_water_level_m") - F.col("dead_water_level_m")) > 0, 
                       (F.col("normal_water_level_m") - F.col("dead_water_level_m"))).otherwise(1.0),
                1
            )
        )
        .withColumn(
            "reservoir_alert_status",
            F.when(F.col("total_gates_open") > 0, F.lit("Đang xả lũ"))
            .when(F.col("upstream_water_level_m") >= (F.col("normal_water_level_m") - 1.5), F.lit("Mực nước cao / Đầy hồ"))
            .when(F.col("upstream_water_level_m") <= (F.col("dead_water_level_m") + 3.0), F.lit("Nguy cơ kiệt nước"))
            .otherwise(F.lit("Vận hành bình thường"))
        )
    )

    # 2. National daily weather summary
    weather_daily = (
        weather_df.alias("w")
        .join(loc_df.alias("l"), F.col("w.location_key") == F.col("l.location_key"), "inner")
        .groupBy("w.date_key")
        .agg(
            F.round(F.avg("w.temperature_2m"), 1).alias("national_avg_temp_c"),
            F.round(F.max("w.temperature_2m"), 1).alias("national_max_temp_c"),
            F.round(F.avg("w.apparent_temperature"), 1).alias("national_apparent_temp_c"),
            F.round(F.avg("w.relative_humidity_2m"), 1).alias("national_humidity_pct"),
            F.round(F.sum("w.precipitation"), 1).alias("national_precipitation_mm"),
            F.round(F.avg("w.shortwave_radiation"), 1).alias("national_solar_radiation_w_m2"),
        )
    )

    # 3. Peak load & Solar gen
    load_daily = (
        load_df.groupBy("date_key").agg(
            F.max("daily_peak_load_mw").alias("peak_load_mw"),
            F.avg("daily_avg_load_mw").alias("avg_load_mw")
        )
    )

    solar_daily = (
        gen_df.alias("g")
        .join(source_df.alias("s"), F.col("g.energy_source_key") == F.col("s.energy_source_key"), "inner")
        .filter(F.lower(F.col("s.source_code")) == "solar")
        .groupBy("g.date_key")
        .agg(F.sum("g.generation_mwh").alias("solar_generation_mwh"))
    )

    result = (
        hydro_enriched.alias("he")
        .join(weather_daily.alias("wd"), F.col("he.date_key") == F.col("wd.date_key"), "left")
        .join(load_daily.alias("ld"), F.col("he.date_key") == F.col("ld.date_key"), "left")
        .join(solar_daily.alias("sd"), F.col("he.date_key") == F.col("sd.date_key"), "left")
        .orderBy("he.date_key", "he.plant_name")
    )

    return result


def save_rollup_table(spark: SparkSession, df: DataFrame, table_name: str) -> None:
    """Save pre-aggregated DataFrame as an optimized Iceberg table in nessie.gold."""
    full_table_name = f"nessie.gold.{table_name}"
    count = df.count()
    logger.info(f"Writing {count} rows to {full_table_name}...")

    if spark.catalog.tableExists(full_table_name):
        df.writeTo(full_table_name).overwrite(F.lit(True))
    else:
        df.writeTo(full_table_name) \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("format-version", "2") \
            .using("iceberg") \
            .create()

    logger.info(f"  [✓] Materialized {full_table_name} successfully.")


def main():
    parser = argparse.ArgumentParser(description="Build Pre-aggregated BI Rollup Tables for Dashboards")
    parser.add_argument("--dashboard", choices=["all", "d1", "d2"], default="all", help="Target dashboard rollups")
    args = parser.parse_args()

    spark = create_spark_session("lakehouse_bi_rollups")
    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.dashboard in ("all", "d1"):
            d1_df = build_dashboard1_supply_demand_rollup(spark)
            save_rollup_table(spark, d1_df, "agg_dashboard1_supply_demand_hourly")

        if args.dashboard in ("all", "d2"):
            d2_df = build_dashboard2_climate_hydro_rollup(spark)
            save_rollup_table(spark, d2_df, "agg_dashboard2_climate_hydro_daily")

        logger.info("\n>>> ALL DASHBOARD ROLLUPS BUILT SUCCESSFULLY! <<<")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
