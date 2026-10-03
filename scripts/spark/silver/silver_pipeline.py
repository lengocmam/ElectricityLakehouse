from pyspark.sql import DataFrame

from silver.clean_generation import clean_generation
from silver.clean_hydro import clean_hydro
from silver.clean_load import clean_load
from silver.clean_power import clean_power
from silver.clean_weather_daily import clean_weather_daily
from silver.clean_weather_hourly import clean_weather_hourly
from silver.extract_evn import extract_evn_generation, extract_evn_power
from silver.extract_evn_hydro import extract_evn_hydro
from silver.extract_nsmo import extract_nsmo_load
from silver.extract_open_meteo import extract_open_meteo_daily, extract_open_meteo_hourly


def power(bronze_df: DataFrame): return clean_power(extract_evn_power(bronze_df))
def generation(bronze_df: DataFrame): return clean_generation(extract_evn_generation(bronze_df))
def hydro(bronze_df: DataFrame): return clean_hydro(extract_evn_hydro(bronze_df))
def load(bronze_df: DataFrame): return clean_load(extract_nsmo_load(bronze_df))
def weather_daily(bronze_df: DataFrame): return clean_weather_daily(extract_open_meteo_daily(bronze_df))
def weather_hourly(bronze_df: DataFrame): return clean_weather_hourly(extract_open_meteo_hourly(bronze_df))
