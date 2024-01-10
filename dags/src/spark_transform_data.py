from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
from src.logger_config import setup_logging
from src.spark_utils import load_dataframe, save_df_as_json, assemble_files_path, DATE, RAW_ROOT_PATH
from schemas.attributes_mapping import COLS_MAPPING, METHODS

logger = setup_logging(__name__)
logger.propagate = False

def transform_raw_data(spark: SparkSession) -> None:
    """Transforms raw data for different methods."""
    logger.info(f'Transformation started.')
    for method in METHODS:
        try:
            logger.info(f'Transforming method {method}.')
            files_path, path_exists = assemble_files_path(RAW_ROOT_PATH, method)
            if not path_exists:
                continue
            original_df = load_dataframe(spark, files_path)
            final_df = process_dataframe(original_df, method)
            save_df_as_json(final_df, method)
        except Exception as e:
            logger.error(f'Exception raised for method {method}:')
            logger.error(e)
    logger.info(f'Transformation finished for data in {DATE}.')


def process_dataframe(df: DataFrame, method: str) -> DataFrame:
    """Process the DataFrame based on the method requested from API."""
    if method == 'current':
        flatten_df = df.select(assemble_flattening_query(method))
        transformed_df = flatten_df.withColumn('last_updated_dttm', to_timestamp("last_updated")) \
            .withColumn('localtime_dttm', to_timestamp('localtime'))
        filtering_df = transformed_df.selectExpr('*',check_subquery())
        filtered_df = filtering_df.where('is_identifiable = True')
        final_df = filtered_df.select(trim_current_query())

    elif method == 'astronomy':
        raise NotImplementedError(f'Method {method} not implemented yet.')
    
    elif method == 'marine':
        raise NotImplementedError(f'Method {method} not implemented yet.')
    
    elif method == 'sports':
        raise NotImplementedError(f'Method {method} not implemented yet.')
    
    else:
        raise ValueError(f'Method {method} is not valid.')
    
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