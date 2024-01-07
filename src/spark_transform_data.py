import os
import json
from typing import List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
from .logger_config import setup_logging
from .spark_utils import load_dataframe
from schemas.attributes_mapping import COLS_MAPPING, METHODS


logger = setup_logging(__name__)
logger.propagate = False

LOAD_ROOT_PATH: str = os.getenv('RAW_PATH')
SAVE_ROOT_PATH: str = os.getenv('PROCESSED_PATH')
DATE: str = datetime.strftime(datetime.now(), '%Y%m%d')

def transform_raw_data(spark: SparkSession) -> None:
    """Transforms raw data for different methods."""
    for method in METHODS:
        try:
            logger.info(f'Transforming method {method}.')
            files_path = f"{LOAD_ROOT_PATH}/{method}/{DATE}"
            if not os.path.exists(files_path):
                logger.info(f'Dataset in {files_path} does not exist')
                continue
            original_df = load_dataframe(spark, files_path)
            final_df = process_dataframe(original_df, method)
            save_as_json(final_df, method)
        except Exception as e:
            logger.error(e)
            logger.error(f'Raised exception for method {method}.')
    logger.info(f'Transformation finished for data in {DATE}.')

def process_dataframe(df: DataFrame, method: str) -> DataFrame:
    """Process the DataFrame based on the method."""
    if method == 'current':
        flatten_df = df.select(assemble_flattening_query(method))
        transformed_df = flatten_df.withColumn('last_updated_dttm', to_timestamp("last_updated")) \
            .withColumn('localtime_dttm', to_timestamp('localtime'))
        filtering_df = transformed_df.selectExpr('*',check_subquery())
        filtered_df = filtering_df.where('is_identifiable = True')
        final_df = filtered_df.select(trim_current_query())
        
    if method == 'astronomy':
        raise NotImplementedError(f'Method {method} not implemented yet.')
    if method == 'marine':
        raise NotImplementedError(f'Method {method} not implemented yet.')
    if method == 'sports':
        raise NotImplementedError(f'Method {method} not implemented yet.')
    return final_df


def assemble_flattening_query(method: str) -> List:
    """Assemble flattening query."""
    aliased_cols = []
    for column in COLS_MAPPING[method].keys():
        aliased_cols += [col(f'{column}.{subcol}').alias(COLS_MAPPING[method][column][subcol])
                         for subcol in COLS_MAPPING[method][column].keys()]
    return aliased_cols

def check_subquery():
    subquery = 'NOT (city IS NULL AND (lat IS NULL OR long IS NULL)) AS is_identifiable'
    return subquery

def trim_current_query() -> List:
    """Trim the current query and select necessary columns."""
    return [
        col('cloud'), col('condition.text').alias('condition'), col('condition.code').alias('condition_code'),
        col('feelslike_C'), col('gust_kph'), col('humidity'), col('is_day'), col('last_updated_dttm').alias('last_updated'),
        col('precip_mm'), col('pressure_in'), col('temp_c'), col('uv'), col('vis_km'), col('wind_degree'),
        col('wind_dir'), col('wind_kph'), col('country'), col('city'), col('region'), col('lat'), col('long'),
        col('localtime_dttm').alias('localtime'), col('tz_id')
    ]


def save_as_json(df: DataFrame, method: str) -> None:
    """Save DataFrame as JSON."""
    json_data = df.toJSON().collect()
    [save_row(row, method) for row in json_data]


def save_row(row: str, method: str) -> None:
    """Save row data as JSON."""
    json_sample = json.loads(row)
    city = json_sample['city']
    create_daily_dir(method)
    file_name = f"{SAVE_ROOT_PATH}/{method}/{DATE}/{city}.json"
    with open(file_name, 'w') as f:
        json.dump(json_sample, f)
        logger.info(f'Saving processed {city}.')


def create_daily_dir(method: str) -> None:
    """Create a daily directory if it doesn't exist."""
    dir_path = f"{SAVE_ROOT_PATH}/{method}/{DATE}"
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
        logger.info('Daily directory created.')


if __name__ == "__main__":
    spark = SparkSession.builder.appName('Spark download app').config('', '').getOrCreate()
    transform_raw_data(spark)