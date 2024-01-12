
import os
import json
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from dags.src.logger_config import setup_logging
logger = setup_logging(__name__)
logger.propagate = False
from dotenv import load_dotenv
load_dotenv()
DATE: datetime = datetime.now()
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
    folder_path = f"{PROCESSED_ROOT_PATH}/{method}"
    folder_path = check_path(folder_path,DATE)
    file_name = f'{folder_path}/{city}.json'
    with open(file_name, 'w') as f:
        json.dump(json_sample, f)
        logger.info(f'Saving processed {city} "{method}".')

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
    files_path = f"{root_path}/{method}/{str(DATE.year)}/{str(DATE.month)}/{str(DATE.day)}/{str(DATE.hour)}"
    path_exists = os.path.exists(files_path)
    if not path_exists:
        logger.error(f'Dataset in {files_path} does not exist.')
        
    return files_path, path_exists

def check_path(storage_path,date)->str:
    """
    Check the existence of the directory
    required for storage.
    """

    check_required_folder(storage_path)
    path = storage_path + f'/{str(date.year)}'
    check_required_folder(path)
    path = path + f'/{str(date.month)}'
    check_required_folder(path)
    path = path + f'/{str(date.day)}'
    check_required_folder(path)
    path = path + f'/{str(date.hour)}' 
    check_required_folder(path)
    return path


def check_required_folder(path: str) -> None:
    """Check and create folder if it doesn't exist."""
    if not os.path.isdir(path):
        os.mkdir(path)
        logger.info(f'Folder {path} created.')
