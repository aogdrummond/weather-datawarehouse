# from live_weather_api import download_live_weather_data
import os
from pyspark.sql import  SparkSession
from src.spark_download_raw_data import download_live_weather_data
from src.spark_transform_data import transform_raw_data
from src.insert_transformed_in_db import insert_data_in_db
JDBC_DRIVER_PATH = os.getenv('JDBC_DRIVER_PATH')

def setup_spark_session() -> SparkSession:
    """Create and configure the SparkSession."""
    spark = SparkSession.builder.appName('Spark Data Warehouse app').config('spark.jars',JDBC_DRIVER_PATH).getOrCreate()
    return spark

if __name__ == "__main__":

    spark = setup_spark_session()
    download_live_weather_data(spark)
    transform_raw_data(spark)
    insert_data_in_db(spark)