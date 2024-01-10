
import os
import json
from dotenv import load_dotenv
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from .logger_config import setup_logging
logger = setup_logging(__name__)
logger.propagate = False

load_dotenv()

DATE: str = datetime.strftime(datetime.now(), '%Y%m%d')
RAW_ROOT_PATH: str = os.getenv('RAW_PATH')
PROCESSED_ROOT_PATH: str = os.getenv('PROCESSED_PATH')

def load_dataframe(spark: SparkSession, files_path: str) -> DataFrame:
    """Load the DataFrame from JSON files."""
    try:
        logger.info(f'Loading dataset from path {files_path}.')
        original_df = spark.read.option("multiline", "true").json(files_path)
        return original_df
    except Exception as e:
        logger.error(e)
        logger.error(f'The dataframe in "{files_path}" could not be loaded.')


def save_df_as_json(df: DataFrame, method: str) -> None:
    """Save DataFrame as JSON."""
    json_data = df.toJSON().collect()
    [save_row(row, method) for row in json_data]


def save_row(row: str, method: str) -> None:
    """Save row data as JSON."""
    json_sample = json.loads(row)
    city = json_sample['city']
    create_daily_dir(method)
    file_name = f"{PROCESSED_ROOT_PATH}/{method}/{DATE}/{city}.json"
    with open(file_name, 'w') as f:
        json.dump(json_sample, f)
        logger.info(f'Saving processed {city} "{method}".')


def create_daily_dir(method: str) -> None:
    """Create a daily directory if it doesn't exist."""
    dir_path = f"{PROCESSED_ROOT_PATH}/{method}/{DATE}"
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
        logger.info(f'Daily directory "{dir_path}" created.')

def assemble_files_path(root_path:str, method:str)->str:
    """
    Assembles the string with path of the dataset saved
    to be parsed, from root_path.

    Args:
    - root_path (str)
    - method(str) 

    Returns:
    - files_path (str)
    - path_exists (bool)
    """
    files_path = f"{root_path}/{method}/{DATE}"
    path_exists = os.path.exists(files_path)
    if not path_exists:
        logger.error(f'Dataset in {files_path} does not exist.')
        
    return files_path, path_exists