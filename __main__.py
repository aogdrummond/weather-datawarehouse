# from live_weather_api import download_live_weather_data
from pyspark.sql import  SparkSession
from src.spark_download_raw_data import download_live_weather_data
from src.spark_transform_data import transform_raw_data


def setup_spark_session() -> SparkSession:
    """Create and configure the SparkSession."""
    spark = SparkSession.builder.appName('Spark download app').config('','').getOrCreate()
    return spark

if __name__ == "__main__":

    spark = setup_spark_session()
    download_live_weather_data(spark)
    transform_raw_data(spark)
    # Next step: insert to db