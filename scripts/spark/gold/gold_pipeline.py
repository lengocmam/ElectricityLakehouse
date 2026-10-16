"""Pipeline mapping for all dimensions and facts in the gold layer."""
from pyspark.sql import DataFrame, SparkSession

from gold.dim_date import generate_dim_date
from gold.dim_time import generate_dim_time
from gold.dim_region import generate_dim_region
from gold.dim_location import generate_dim_location
from gold.dim_plant import generate_dim_plant
from gold.dim_energy_source import generate_dim_energy_source
from gold.dim_period import generate_dim_period

from gold.fact_load_hourly import transform_fact_load_hourly
from gold.fact_weather_hourly import transform_fact_weather_hourly
from gold.fact_weather_daily import transform_fact_weather_daily
from gold.fact_hydro_status import transform_fact_hydro_status
from gold.fact_generation_daily import transform_fact_generation_daily
from gold.fact_generation_capacity_daily import transform_fact_generation_capacity_daily


def get_dimension(spark: SparkSession, dim_name: str) -> DataFrame:
    """Load or generate dimension table DataFrame."""
    gold_table = f"nessie.gold.{dim_name}"
    if spark.catalog.tableExists(gold_table):
        return spark.table(gold_table)

    generators = {
        "dim_date": lambda: generate_dim_date(spark),
        "dim_time": lambda: generate_dim_time(spark),
        "dim_region": lambda: generate_dim_region(spark),
        "dim_location": lambda: generate_dim_location(spark),
        "dim_plant": lambda: generate_dim_plant(spark),
        "dim_energy_source": lambda: generate_dim_energy_source(spark),
        "dim_period": lambda: generate_dim_period(spark),
    }

    if dim_name not in generators:
        raise ValueError(f"Unknown dimension: {dim_name}")

    return generators[dim_name]()


def load_hourly(spark: SparkSession, source_df: DataFrame) -> DataFrame:
    dim_region_df = get_dimension(spark, "dim_region")
    return transform_fact_load_hourly(source_df, dim_region_df)


def weather_hourly(spark: SparkSession, source_df: DataFrame) -> DataFrame:
    dim_location_df = get_dimension(spark, "dim_location")
    return transform_fact_weather_hourly(source_df, dim_location_df)


def weather_daily(spark: SparkSession, source_df: DataFrame) -> DataFrame:
    dim_location_df = get_dimension(spark, "dim_location")
    return transform_fact_weather_daily(source_df, dim_location_df)


def hydro_status(spark: SparkSession, source_df: DataFrame) -> DataFrame:
    dim_plant_df = get_dimension(spark, "dim_plant")
    return transform_fact_hydro_status(source_df, dim_plant_df)


def generation_daily(spark: SparkSession, source_df: DataFrame) -> DataFrame:
    return transform_fact_generation_daily(source_df)


def generation_capacity_daily(spark: SparkSession, source_df: DataFrame) -> DataFrame:
    return transform_fact_generation_capacity_daily(source_df)
