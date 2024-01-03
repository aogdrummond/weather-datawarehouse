import os
import json
from datetime import datetime
from attributes_mapping import COLS_MAPPING, METHODS
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.types import BooleanType

LOAD_ROOT_PATH = 'storage/raw'
SAVE_ROOT_PATH = 'storage/processed'
DATE = datetime.strftime(datetime.now(),'%Y%m%d')

#Include logging and replace prints

def transform_raw_data(spark):
    
    for method in METHODS:
        try:
            print(f'Transforming method {method}.')
            files_path = f"{LOAD_ROOT_PATH}/{method}/{DATE}"
            if not os.path.exists(files_path):
                print(f'Dataset in {files_path} does not exist')
                continue
            original_df = load_dataframe(spark,files_path)
            final_df = process_dataframe(original_df,method)
            #Criar amostra pra checar a filtragem    
            save_as_json(final_df,method)
        except Exception as e:
            print(e)
            print(f'Raised exception for method {method}.')
    print(f'Transformation finished for data in {DATE}.')

def load_dataframe(spark,files_path):
    try:
        original_df = spark.read.option("multiline","true").json(files_path)
        return original_df
    except Exception as e:
        print(e)
        print('The dataframe could not be loaded')

def process_dataframe(df,method):
    """
    """
    if method == 'current':
        flatten_df = df.select(assemble_flattening_query(method))
        transformed_df = flatten_df.withColumn('last_updated_dttm',to_timestamp("last_updated"))\
                                .withColumn('localtime_dttm',to_timestamp('localtime'))    
        udf_ident_verification = udf(verify_if_is_identifiable, BooleanType())
        filtering_df = transformed_df.withColumn("is_identifiable", udf_ident_verification(col("city"), col("lat"), col("long")))
        filtered_df = filtering_df.where('is_identifiable = True')
        final_df = filtered_df.select(trim_current_query())
    return final_df


def assemble_flattening_query(method:str):
    """
    Flatten DataFrame, keeping only required columns as set in the dictionary "COLS_MAPPING"
    """
    aliased_cols = []
    for column in COLS_MAPPING[method].keys():
        aliased_cols += [col(f'{column}.{subcol}').alias(COLS_MAPPING[method][column][subcol]) for subcol in COLS_MAPPING[method][column].keys()]
    return aliased_cols

def verify_if_is_identifiable(city,lat,long):

    is_identifiable = False
    if (city is not None):
        is_identifiable = True
    elif (lat is not None and long is not None):
        is_identifiable = True
    return is_identifiable

def trim_current_query():
    
    return [col('cloud'),col('condition.text').alias('condition'),col('condition.code').alias('condition_code'),
            col('feelslike_C'),col('gust_kph'),col('humidity'),col('is_day'),col('last_updated_dttm').alias('last_updated'),
            col('precip_mm'),col('pressure_in'), col('temp_c'),col('uv'),col('vis_km'),col('wind_degree'),col('wind_dir'),
            col('wind_kph'),col('country'),col('city'),col('region'),col('lat'),col('long'),col('localtime_dttm').alias('localtime'),
            col('tz_id')]


def save_as_json(df,method):
    json_data = df.toJSON().collect()
    [save_row(row,method) for row in json_data]

def save_row(row:str,method):

    json_sample = json.loads(row)
    city = json_sample['city']
    create_daily_dir(method)
    file_name = f"{SAVE_ROOT_PATH}/{method}/{DATE}/{city}.json"
    with open(file_name,'w') as f:
        json.dump(json_sample,f)
        print(f'Saving processed {city}.')

def create_daily_dir(method):
    dir_path = f"{SAVE_ROOT_PATH}/{method}/{DATE}"
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
        print('Daily directory created.')


if __name__ == "__main__":
    
    spark = SparkSession.builder.appName('Spark download app').config('','').getOrCreate()
    transform_raw_data(spark)