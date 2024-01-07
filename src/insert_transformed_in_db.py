import os
from pyspark.sql import SparkSession, DataFrame
from .db_connector import DbCursor
from .logger_config import setup_logging
from datetime import datetime
from pyspark.sql.functions import col, when, udf, regexp_replace, current_timestamp
from pyspark.sql.types import TimestampType, IntegerType

logger = setup_logging(__name__)
logger.propagate = False
LOAD_ROOT_PATH: str = 'storage/processed'
DATE: str = datetime.strftime(datetime.now(), '%Y%m%d')

dbcursor = DbCursor()

def insert_data_in_db(spark):

    try:
        files_path = f"{LOAD_ROOT_PATH}/current/{DATE}"
        if not os.path.exists(files_path):
            logger.info(f'Dataset in {files_path} does not exist')
        processed_df = load_dataframe(spark,files_path)
        formatted_db = format_dataframe_to_db(processed_df)
        # persist_to_db(formatted_db)
        print()
    
    except Exception as e:
        print(e)

def load_dataframe(spark: SparkSession, files_path: str) -> DataFrame:
    """Load the DataFrame from JSON files."""
    try:
        original_df = spark.read.option("multiline", "true").json(files_path)
        return original_df
    except Exception as e:
        logger.error(e)
        logger.error('The dataframe could not be loaded')

def format_dataframe_to_db(df:DataFrame)->DataFrame:

    locations_df = df.selectExpr('city','country','region','lat','long','tz_id')
    weathers_df = df.selectExpr('city','cloud','condition','feelslike_C','gust_kph','humidity',\
                                'last_updated','localtime','precip_mm','pressure_in','temp_c',\
                                    'uv','vis_km','wind_degree','wind_dir','wind_kph')
    trans_locations_df = transform_locations(locations_df)
    verify_location_udf = udf(verify_location,IntegerType())
    trans_locations_df_2 = trans_locations_df.withColumn('id',verify_location_udf(trans_locations_df['city']\
                                                         ,trans_locations_df['lat']\
                                                         ,trans_locations_df['long']))
    trans_location_df_3 = trans_locations_df_2.selectExpr('*','id is not NULL as is_identified')
    #Insert new locations

    new_locations_df = trans_location_df_3.filter(col('is_identified')==False)
    inserted_locations_df = new_locations_df.drop('id','is_identified').withColumn('created_at',current_timestamp())
    persist_to_db(inserted_locations_df,'locations')
    trans_weathers_df = tranform_weathers(weathers_df)
    trans_weathers_df_2 = trans_weathers_df.withColumn('location_id',verify_location_udf(trans_weathers_df['city']))      
    inserted_weathers_df = trans_weathers_df_2.drop('city').withColumn('created_at',current_timestamp())
    persist_to_db(inserted_weathers_df,'weathers')

def transform_locations(df:DataFrame)->DataFrame:

    #Drop empty strings
    df_without_empty_str = df.withColumn("city", when(col("city") == '', None).otherwise(col("city"))) \
                  .withColumn("country", when(col("country") == '', None).otherwise(col("country"))) \
                  .withColumn("region", when(col("region") == '', None).otherwise(col("region")))\
                  .withColumn("tz_id", when(col("tz_id") == '', None).otherwise(col("tz_id")))
    df_without_single_quotes = df_without_empty_str.withColumn("city",regexp_replace('city',"\'",""))\
                                        .withColumn("country",regexp_replace('country',"\'",""))\
                                        .withColumn("region",regexp_replace('region',"\'",""))\
                                        .withColumn("tz_id",regexp_replace('tz_id',"\'",""))
    return df_without_single_quotes

def tranform_weathers(df:DataFrame)->DataFrame:

    transformed_df = df.withColumn("condition", when(col("condition") == '', None).otherwise(col("condition"))) \
                  .withColumn("wind_dir", when(col("wind_dir") == '', None).otherwise(col("wind_dir"))) \
                  .withColumn("last_updated",col('last_updated').cast(TimestampType())) \
                  .withColumn("localtime",col('localtime').cast(TimestampType()))\
                  .withColumn("city",regexp_replace('city',"\'",""))
    return transformed_df

def verify_location(city,lat='NULL',long='NULL')->int:

    response = dbcursor.fetch_location(city,lat,long)
    return response

def fetch_location(city)->int:

    response = dbcursor.fetch_location(city)
    return response

def persist_to_db(df:DataFrame,table)->DataFrame:

    df.write.mode('append').format('jdbc')\
        .option('url',dbcursor.url,)\
        .option('driver',dbcursor.driver)\
        .option('dbtable',table)\
        .option('user',dbcursor.user)\
        .option('password',dbcursor.password).save()