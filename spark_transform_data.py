import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, array
from pyspark.sql.types import BooleanType

SAVE_PATH = 'storage/processed/current'
LOAD_PATH = 'storage/raw/current'

def transform_raw_data(spark):
    date = datetime.strftime(datetime.now(),'%Y%m%d')
    files_path = f"{LOAD_PATH}/{date}"
    current_df = spark.read.option("multiline","true").json(files_path)
    flatten_df = current_df.select(assemble_flattening_query())
    transformed_df = flatten_df.withColumn('last_updated_dttm',to_timestamp("last_updated"))\
                                .withColumn('localtime_dttm',to_timestamp('localtime'))    
    udf_ident_verification = udf(verify_if_is_identifiable, BooleanType())
    filtering_df = transformed_df.withColumn("is_identifiable", udf_ident_verification(col("city"), col("lat"), col("long")))
    filtered_df = filtering_df.where('is_identifiable = True')
    trimmed_df = filtered_df.select(assemble_trimming_query())
    #Criar amostra pra checar a filtragem    
    save_as_json(trimmed_df)

def assemble_flattening_query():
    """
    Flatten DataFrame, keeping only required columns as set in the dictionary "cols_mapping"
    """
    aliased_cols = []
    for column in cols_mapping.keys():
        aliased_cols += [col(f'{column}.{subcol}').alias(cols_mapping[column][subcol]) for subcol in cols_mapping[column].keys()]
    return aliased_cols

cols_mapping = {'current':{'cloud':'cloud',
                'condition':'condition',
                'feelslike_c':'feelslike_C',
                'gust_kph':'gust_kph',
                'humidity':'humidity',
                'is_day':'is_day',
                'last_updated':'last_updated',
                'precip_mm':'precip_mm',
                'pressure_in':'pressure_in',
                'temp_c':'temp_C',
                'uv':'uv',
                'vis_km':'vis_km',
                'wind_degree':'wind_degree',
                'wind_dir':'wind_dir',
                'wind_kph':'wind_kph'},
                'location':{'country':'country',
                'name':'city',
                'region':'region',
                'lat':'lat',
                'lon':'long',
                'localtime':'localtime',
                'tz_id':'tz_id'}}


def verify_if_is_identifiable(city,lat,long):

    is_identifiable = False
    if (city is not None):
        is_identifiable = True
    elif (lat is not None and long is not None):
        is_identifiable = True
    return is_identifiable

def assemble_trimming_query():
    
    return [col('cloud'),col('condition.text').alias('condition'),col('condition.code').alias('condition_code'),
            col('feelslike_C'),col('gust_kph'),col('humidity'),col('is_day'),col('last_updated_dttm').alias('last_updated'),
            col('precip_mm'),col('pressure_in'), col('temp_c'),col('uv'),col('vis_km'),col('wind_degree'),col('wind_dir'),
            col('wind_kph'),col('country'),col('city'),col('region'),col('lat'),col('long'),col('localtime_dttm').alias('localtime'),
            col('tz_id')]


def save_as_json(df):
    json_data = df.toJSON().collect()
    [save_row(row) for row in json_data]

def save_row(row:str):

    json_sample = json.loads(row)
    city = json_sample['city']
    #Encontrar solução melhor para isso aqui, já que tá pegando o instante
    #atual ao invés do date de extração raw
    date = datetime.strftime(datetime.now(),'%Y%m%d')
    create_daily_dir(date)
    file_name = f"{SAVE_PATH}/{date}/{city}.json"
    with open(file_name,'w') as f:
        json.dump(json_sample,f)
        print(f'Saving processed {city}.')

def create_daily_dir(date):
    dir_path = f"{SAVE_PATH}/{date}"
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
        print('Daily directory created.')


if __name__ == "__main__":
    spark = SparkSession.builder.appName('Spark download app').config('','').getOrCreate()
    transform_raw_data(spark)