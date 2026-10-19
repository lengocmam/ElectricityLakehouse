"""Machine Learning Data Export & Feature Engineering Pipeline for Lakehouse.

Exports Lakehouse Gold Star Schema data into ML-ready tabular formats (CSV, Parquet)
specifically designed for Time-Series Forecasting & Deep Learning training:
1. ml_load_forecasting_hourly: National & Regional electricity load with weather & lag features.
2. ml_hydro_inflow_daily: Reservoir water levels, inflows, discharges & rainfall metrics.
3. ml_generation_mix_daily: Generation output & renewable shares across power sources.
4. Raw Star Schema Tables: Individual Fact and Dimension tables for custom modeling.
"""
import argparse
import math
import os
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F

from utils.spark import create_spark_session
from utils.logging import create_logger

logger = create_logger(__name__)

GOLD_FACTS = [
    "fact_load_hourly",
    "fact_weather_hourly",
    "fact_weather_daily",
    "fact_hydro_status",
    "fact_generation_daily",
    "fact_generation_capacity_daily",
]

GOLD_DIMS = [
    "dim_date",
    "dim_time",
    "dim_region",
    "dim_location",
    "dim_plant",
    "dim_energy_source",
    "dim_period",
]


def _save_dataframe(df: DataFrame, output_path_base: str, export_format: str = "both") -> None:
    """Save PySpark DataFrame as single clean CSV and/or Parquet file."""
    os.makedirs(os.path.dirname(output_path_base), exist_ok=True)

    if export_format in ("parquet", "both"):
        parquet_file = f"{output_path_base}.parquet"
        logger.info(f"Saving Parquet -> {parquet_file}...")
        df.write.mode("overwrite").parquet(parquet_file)

    if export_format in ("csv", "both"):
        csv_file = f"{output_path_base}.csv"
        logger.info(f"Saving single CSV -> {csv_file}...")
        # Convert to pandas or coalesce to 1 to write a single clean CSV file
        try:
            pdf = df.toPandas()
            pdf.to_csv(csv_file, index=False, encoding="utf-8")
            logger.info(f"  [✓] Wrote {len(pdf)} rows to {csv_file}")
        except Exception as exc:
            logger.warning(f"Fallback to Spark CSV writer: {exc}")
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_file)


def build_load_forecasting_dataset(spark: SparkSession) -> DataFrame:
    """
    Build ML-ready Feature Engineering dataset for Electricity Load Forecasting.
    
    Includes:
    - Target: load_mw, load_target_lead_24h (for Day-Ahead forecasting)
    - Temporal & Cyclical features (sin/cos of hour, day, month)
    - Regional average weather features (temperature, humidity, solar radiation, wind)
    - Time-series Lags: 1h, 2h, 24h (yesterday), 48h, 168h (same hour last week)
    - Rolling window statistics: 24h mean, stddev, min, max
    """
    logger.info("Building ML Load Forecasting Dataset...")

    load_df = spark.table("nessie.gold.fact_load_hourly")
    date_df = spark.table("nessie.gold.dim_date")
    time_df = spark.table("nessie.gold.dim_time")
    region_df = spark.table("nessie.gold.dim_region")

    # 1. Join Base Dimensions
    joined = (
        load_df.alias("l")
        .join(date_df.alias("d"), F.col("l.date_key") == F.col("d.date_key"), "inner")
        .join(time_df.alias("t"), F.col("l.time_key") == F.col("t.time_key"), "inner")
        .join(region_df.alias("r"), F.col("l.region_key") == F.col("r.region_key"), "inner")
        .select(
            F.col("l.date_key"),
            F.col("l.time_key"),
            F.col("l.region_key"),
            F.col("r.region_name"),
            F.col("d.full_date"),
            F.col("d.year"),
            F.col("d.month"),
            F.col("d.day"),
            F.col("d.day_of_week"),
            F.col("d.day_name"),
            F.col("d.is_weekend"),
            F.col("d.season"),
            F.col("t.hour"),
            F.col("t.time_period"),
            F.col("l.load_mw"),
            F.col("l.daily_avg_load_mw"),
            F.col("l.daily_peak_load_mw"),
            F.col("l.daily_min_load_mw"),
        )
    )

    # 2. Join Regional Weather (Aggregated across provinces in each region)
    if spark.catalog.tableExists("nessie.gold.fact_weather_hourly") and spark.catalog.tableExists("nessie.gold.dim_location"):
        weather_df = spark.table("nessie.gold.fact_weather_hourly")
        loc_df = spark.table("nessie.gold.dim_location")

        weather_regional = (
            weather_df.alias("w")
            .join(loc_df.alias("loc"), F.col("w.location_key") == F.col("loc.location_key"), "inner")
            .groupBy("w.date_key", "w.time_key", "loc.region_key")
            .agg(
                F.round(F.avg("w.temperature_2m"), 2).alias("weather_temp_c"),
                F.round(F.avg("w.apparent_temperature"), 2).alias("weather_apparent_temp_c"),
                F.round(F.avg("w.relative_humidity_2m"), 2).alias("weather_humidity_pct"),
                F.round(F.avg("w.precipitation"), 2).alias("weather_precipitation_mm"),
                F.round(F.avg("w.cloud_cover"), 2).alias("weather_cloud_cover_pct"),
                F.round(F.avg("w.shortwave_radiation"), 2).alias("weather_solar_radiation_w_m2"),
                F.round(F.avg("w.wind_speed_10m"), 2).alias("weather_wind_speed_m_s"),
            )
        )

        joined = joined.join(
            weather_regional,
            (joined.date_key == weather_regional.date_key) &
            (joined.time_key == weather_regional.time_key) &
            (joined.region_key == weather_regional.region_key),
            "left"
        ).drop(weather_regional.date_key).drop(weather_regional.time_key).drop(weather_regional.region_key)

    # 3. Cyclical Temporal Features (Sine & Cosine transforms for Neural Nets & ML)
    two_pi = 2.0 * math.pi
    joined = (
        joined
        .withColumn("hour_sin", F.round(F.sin(F.col("hour") * F.lit(two_pi / 24.0)), 4))
        .withColumn("hour_cos", F.round(F.cos(F.col("hour") * F.lit(two_pi / 24.0)), 4))
        .withColumn("day_of_week_sin", F.round(F.sin(F.col("day_of_week") * F.lit(two_pi / 7.0)), 4))
        .withColumn("day_of_week_cos", F.round(F.cos(F.col("day_of_week") * F.lit(two_pi / 7.0)), 4))
        .withColumn("month_sin", F.round(F.sin(F.col("month") * F.lit(two_pi / 12.0)), 4))
        .withColumn("month_cos", F.round(F.cos(F.col("month") * F.lit(two_pi / 12.0)), 4))
    )

    # 4. Time Series Lag & Rolling Window Features
    w_lag = Window.partitionBy("region_key").orderBy("date_key", "time_key")
    w_roll_24h = Window.partitionBy("region_key").orderBy("date_key", "time_key").rowsBetween(-23, 0)

    featured_df = (
        joined
        .withColumn("load_lag_1h", F.lag("load_mw", 1).over(w_lag))
        .withColumn("load_lag_2h", F.lag("load_mw", 2).over(w_lag))
        .withColumn("load_lag_24h", F.lag("load_mw", 24).over(w_lag))
        .withColumn("load_lag_48h", F.lag("load_mw", 48).over(w_lag))
        .withColumn("load_lag_168h", F.lag("load_mw", 168).over(w_lag))  # Same hour 1 week ago
        .withColumn("load_rolling_mean_24h", F.round(F.avg("load_mw").over(w_roll_24h), 2))
        .withColumn("load_rolling_std_24h", F.round(F.stddev("load_mw").over(w_roll_24h), 2))
        .withColumn("load_rolling_min_24h", F.min("load_mw").over(w_roll_24h))
        .withColumn("load_rolling_max_24h", F.max("load_mw").over(w_roll_24h))
        # Day-Ahead Lead Target (T+24 forecast target)
        .withColumn("load_target_lead_24h", F.lead("load_mw", 24).over(w_lag))
        .orderBy("region_key", "date_key", "time_key")
    )

    return featured_df


def build_hydro_inflow_dataset(spark: SparkSession) -> DataFrame:
    """Build ML dataset for Hydropower Reservoir & Inflow Forecasting."""
    logger.info("Building ML Hydro Reservoir & Inflow Dataset...")

    hydro_df = spark.table("nessie.gold.fact_hydro_status")
    plant_df = spark.table("nessie.gold.dim_plant")
    date_df = spark.table("nessie.gold.dim_date")

    joined = (
        hydro_df.alias("h")
        .join(plant_df.alias("p"), F.col("h.plant_key") == F.col("p.plant_key"), "inner")
        .join(date_df.alias("d"), F.col("h.date_key") == F.col("d.date_key"), "inner")
        .select(
            F.col("h.date_key"),
            F.col("d.full_date"),
            F.col("d.month"),
            F.col("d.season"),
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
        )
    )

    # Inflow Lags and Rolling Average
    w_plant = Window.partitionBy("plant_name").orderBy("date_key")
    w_roll_7d = Window.partitionBy("plant_name").orderBy("date_key").rowsBetween(-6, 0)

    featured_df = (
        joined
        .withColumn("inflow_lag_1d", F.lag("reservoir_inflow_m3_per_s", 1).over(w_plant))
        .withColumn("inflow_lag_2d", F.lag("reservoir_inflow_m3_per_s", 2).over(w_plant))
        .withColumn("inflow_lag_7d", F.lag("reservoir_inflow_m3_per_s", 7).over(w_plant))
        .withColumn("inflow_rolling_mean_7d", F.round(F.avg("reservoir_inflow_m3_per_s").over(w_roll_7d), 2))
        .withColumn("water_level_change_1d", F.round(F.col("upstream_water_level_m") - F.lag("upstream_water_level_m", 1).over(w_plant), 2))
        .orderBy("plant_name", "date_key")
    )

    return featured_df


def build_generation_mix_dataset(spark: SparkSession) -> DataFrame:
    """Build ML dataset for Power Generation Output & Source Mix."""
    logger.info("Building ML Generation Mix Dataset...")

    gen_df = spark.table("nessie.gold.fact_generation_daily")
    src_df = spark.table("nessie.gold.dim_energy_source")
    date_df = spark.table("nessie.gold.dim_date")

    joined = (
        gen_df.alias("g")
        .join(src_df.alias("s"), F.col("g.energy_source_key") == F.col("s.energy_source_key"), "inner")
        .join(date_df.alias("d"), F.col("g.date_key") == F.col("d.date_key"), "inner")
        .select(
            F.col("g.date_key"),
            F.col("d.full_date"),
            F.col("d.month"),
            F.col("d.day_of_week"),
            F.col("d.is_weekend"),
            F.col("d.season"),
            F.col("s.source_code"),
            F.col("s.source_name"),
            F.col("s.renewable_flag"),
            F.col("g.generation_mwh"),
            F.col("g.generation_share_pct"),
            F.col("g.previous_day_generation_mwh"),
            F.col("g.generation_change_mwh"),
            F.col("g.generation_growth_pct"),
            F.col("g.source_rank_in_day"),
        )
        .orderBy("date_key", "source_rank_in_day")
    )

    return joined


def export_raw_gold_tables(spark: SparkSession, output_dir: str, export_format: str = "both") -> None:
    """Export all individual Star Schema Dimension and Fact tables."""
    gold_dir = os.path.join(output_dir, "gold_tables")
    os.makedirs(gold_dir, exist_ok=True)

    tables = [(tbl, f"nessie.gold.{tbl}") for tbl in GOLD_DIMS + GOLD_FACTS]

    for name, full_tbl in tables:
        if spark.catalog.tableExists(full_tbl):
            logger.info(f"Exporting raw Gold table: {full_tbl}...")
            df = spark.table(full_tbl)
            out_base = os.path.join(gold_dir, name)
            _save_dataframe(df, out_base, export_format=export_format)
        else:
            logger.warning(f"Table {full_tbl} does not exist. Skipped.")


def main():
    default_out = os.environ.get("EXPORT_OUTPUT_DIR")
    if not default_out:
        default_out = "/opt/lakehouse/data_exports" if os.path.exists("/opt/lakehouse/data_exports") else "data_exports"

    parser.add_argument(
        "--output-dir",
        default=default_out,
        help="Target folder path to save exported files (default: auto)",
    )
    parser.add_argument(
        "--dataset",
        choices=["all", "ml_load", "ml_hydro", "ml_generation", "gold_tables"],
        default="all",
        help="Dataset to export (default: all)",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "parquet", "both"],
        default="both",
        help="Export file format: csv, parquet, or both (default: both)",
    )
    args = parser.parse_args()

    spark = create_spark_session("lakehouse_ml_export")
    spark.sparkContext.setLogLevel("WARN")

    out_root = os.path.abspath(args.output_dir)
    ml_dir = os.path.join(out_root, "ml_ready")
    os.makedirs(ml_dir, exist_ok=True)

    logger.info(f"Initiating Lakehouse Export to: {out_root} (Format: {args.format})")

    try:
        # 1. ML Load Forecasting Dataset
        if args.dataset in ("all", "ml_load"):
            if spark.catalog.tableExists("nessie.gold.fact_load_hourly"):
                load_ml_df = build_load_forecasting_dataset(spark)
                _save_dataframe(load_ml_df, os.path.join(ml_dir, "ml_load_forecasting_hourly"), export_format=args.format)
            else:
                logger.warning("fact_load_hourly not found. Skipped ml_load.")

        # 2. ML Hydro Inflow Dataset
        if args.dataset in ("all", "ml_hydro"):
            if spark.catalog.tableExists("nessie.gold.fact_hydro_status"):
                hydro_ml_df = build_hydro_inflow_dataset(spark)
                _save_dataframe(hydro_ml_df, os.path.join(ml_dir, "ml_hydro_inflow_daily"), export_format=args.format)
            else:
                logger.warning("fact_hydro_status not found. Skipped ml_hydro.")

        # 3. ML Generation Mix Dataset
        if args.dataset in ("all", "ml_generation"):
            if spark.catalog.tableExists("nessie.gold.fact_generation_daily"):
                gen_ml_df = build_generation_mix_dataset(spark)
                _save_dataframe(gen_ml_df, os.path.join(ml_dir, "ml_generation_mix_daily"), export_format=args.format)
            else:
                logger.warning("fact_generation_daily not found. Skipped ml_generation.")

        # 4. Raw Gold Star Schema Tables
        if args.dataset in ("all", "gold_tables"):
            export_raw_gold_tables(spark, out_root, export_format=args.format)

        logger.info(f"\n[✓] Export completed successfully! All files are available in: {out_root}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
