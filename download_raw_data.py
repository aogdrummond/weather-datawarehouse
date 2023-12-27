# from live_weather_api import download_live_weather_data
from spark_download_raw_data import download_live_weather_data
from pyspark.sql import  SparkSession
def setup_spark_session() -> SparkSession:
    """Create and configure the SparkSession."""
    spark = SparkSession.builder.appName('Spark download app').config('','').getOrCreate()
    return spark

if __name__ == "__main__":
    import time
    start = time.time()
    spark = setup_spark_session()

    download_live_weather_data(spark)
    end = time.time()
    print(f"Execution time: {end-start}")    
    #download_historic_weather_data...
    #each api is implemented independently, but they are all called here