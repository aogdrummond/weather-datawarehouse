from pyspark.sql import SparkSession, DataFrame
from .logger_config import setup_logging
logger = setup_logging(__name__)

def load_dataframe(spark: SparkSession, files_path: str) -> DataFrame:
    """Load the DataFrame from JSON files."""
    try:
        original_df = spark.read.option("multiline", "true").json(files_path)
        return original_df
    except Exception as e:
        logger.error(e)
        logger.error('The dataframe could not be loaded')