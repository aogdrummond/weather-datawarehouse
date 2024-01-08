from typing import Union
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, udf, regexp_replace, current_timestamp
from pyspark.sql.types import TimestampType, IntegerType
from .db_connector import DbCursor
from .logger_config import setup_logging
from .spark_utils import load_dataframe, assemble_files_path, PROCESSED_ROOT_PATH, DATE

logger = setup_logging(__name__)
logger.propagate = False
dbcursor = DbCursor()


def insert_data_in_db(spark) -> None:
    """
    Insert json processed data located in LOAD_ROOT_PATH
    into the database.

    Args:
    - spark (SparkSession): Spark session object.

    Returns:
    - None
    """
    logger.info('Database data insertion script initialized.')
    try:
        method = 'current'
        files_path, path_exists = assemble_files_path(PROCESSED_ROOT_PATH,method)
        if not path_exists:
            raise ValueError(f'Files path {files_path} does not exist')
        processed_df = load_dataframe(spark, files_path)
        locations_df = parse_locations_df(processed_df)
        persist_to_db(locations_df, "locations")
        weathers_df = parse_weathers_df(processed_df)
        persist_to_db(weathers_df, "weathers")
        logger.info(f'Database insertion succesfully finished for execution {DATE}.')
    except Exception as e:
        logger.error(f'Error on insertion of {method}.')
        logger.error(e)


def parse_locations_df(df: DataFrame) -> DataFrame:
    """
    Parse locations data from the original DataFrame.

    Args:
    - df (DataFrame): Input DataFrame containing processed information.

    Returns:
    - DataFrame: Processed DataFrame with information of locations to be inserted.
    """
    raw_locations_df = df.selectExpr(
        "city", "country", "region", "lat", "long", "tz_id"
    )
    trans_locations_df = transform_data_in_locations(raw_locations_df)
    verify_location_udf = udf(verify_location, IntegerType())
    locations_df_with_id = trans_locations_df.withColumn(
        "id",
        verify_location_udf(
            trans_locations_df["city"],
            trans_locations_df["lat"],
            trans_locations_df["long"],
        ),
    )
    identified_location_df = locations_df_with_id.selectExpr(
        "*", "id is not NULL as is_identified"
    )
    new_locations_df = identified_location_df.filter(col("is_identified") == False)
    insert_locations_df = new_locations_df.drop("id", "is_identified").withColumn(
        "created_at", current_timestamp()
    )
    return insert_locations_df


def transform_data_in_locations(df: DataFrame) -> DataFrame:
    """
    Transform data type and values related to locations in the given DataFrame.

    Args:
    - df (DataFrame): Input DataFrame containing location information.

    Returns:
    - DataFrame: Transformed DataFrame with modified location information.
    """
    df_without_empty_str = (
        df.withColumn("city", when(col("city") == "", None).otherwise(col("city")))
        .withColumn(
            "country", when(col("country") == "", None).otherwise(col("country"))
        )
        .withColumn("region", when(col("region") == "", None).otherwise(col("region")))
        .withColumn("tz_id", when(col("tz_id") == "", None).otherwise(col("tz_id")))
    )
    df_without_single_quotes = (
        df_without_empty_str.withColumn("city", regexp_replace("city", "'", ""))
        .withColumn("country", regexp_replace("country", "'", ""))
        .withColumn("region", regexp_replace("region", "'", ""))
        .withColumn("tz_id", regexp_replace("tz_id", "'", ""))
    )
    return df_without_single_quotes


def verify_location(
    city: str, lat: Union[str, None] = None, long: Union[str, None] = None
) -> int:
    """
    Fetch location ID from database based on city, latitude, and longitude.

    Args:
    - city (str): City name.
    - lat (str or None, optional): Latitude. Defaults to None.
    - long (str or None, optional): Longitude. Defaults to None.

    Returns:
    - int: Location ID.
    """
    response = dbcursor.fetch_location(city, lat, long)
    return response


def parse_weathers_df(df: DataFrame) -> DataFrame:
    """
    Parse weather-related data from the original DataFrame.

    Args:
    - df (DataFrame): Input DataFrame containing processed information.

    Returns:
    - DataFrame: Processed DataFrame with weather information and location id.
    """
    weathers_df = df.selectExpr(
        "city",
        "cloud",
        "condition",
        "feelslike_C",
        "gust_kph",
        "humidity",
        "last_updated",
        "localtime",
        "precip_mm",
        "pressure_in",
        "temp_c",
        "uv",
        "vis_km",
        "wind_degree",
        "wind_dir",
        "wind_kph",
    )

    trans_weathers_df = tranform_data_in_weathers(weathers_df)
    verify_location_udf = udf(verify_location, IntegerType())
    weathers_df_with_id = trans_weathers_df.withColumn(
        "location_id", verify_location_udf(trans_weathers_df["city"])
    )
    inserted_weathers_df = weathers_df_with_id.drop("city").withColumn(
        "created_at", current_timestamp()
    )
    return inserted_weathers_df


def tranform_data_in_weathers(df: DataFrame) -> DataFrame:
    """
    Transform data type and values related  to weather in the given DataFrame.

    Args:
    - df (DataFrame): Input DataFrame containing weather information.

    Returns:
    - DataFrame: Transformed DataFrame with modified weather information.
    """
    transformed_df = (
        df.withColumn(
            "condition", when(col("condition") == "", None).otherwise(col("condition"))
        )
        .withColumn(
            "wind_dir", when(col("wind_dir") == "", None).otherwise(col("wind_dir"))
        )
        .withColumn("last_updated", col("last_updated").cast(TimestampType()))
        .withColumn("localtime", col("localtime").cast(TimestampType()))
        .withColumn("city", regexp_replace("city", "'", ""))
    )

    return transformed_df


def persist_to_db(df: DataFrame, table_name: str) -> None:
    """
    Persist DataFrame to the database, appending the values of 'df'
    to the table 'table_name'.

    Args:
    - df (DataFrame): DataFrame to be persisted.
    - table_name (str): Name of the database table to receive the DataFrame.

    Returns:
    - None
    """

    df.write.mode("append").format("jdbc")\
        .option("url",dbcursor.url)\
        .option("driver", dbcursor.driver)\
        .option("dbtable", table_name)\
        .option("user", dbcursor.user)\
        .option("password", dbcursor.password).save()
    logger.info(f'Data inserted into table "{table_name}"')