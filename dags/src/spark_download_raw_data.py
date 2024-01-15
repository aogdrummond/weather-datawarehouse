import os
import json
from dotenv import load_dotenv
from jsonschema import validate
from typing import List, Tuple, Dict
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType
from pyspark.sql import Row
from dags.src.external_api import request_from_weather_api
from dags.src.logger_config import setup_logging
from dags.src.spark_utils import check_path, RAW_ROOT_PATH, DATE
from dags.schemas.pyspark_schemas import REQUEST_SCHEMA

load_dotenv()
logger = setup_logging(__name__)
logger.propagate = False

CITIES_JSON_PATH = os.getenv("CITIES_JSON_PATH")
RAW_SCHEMA_PATH = os.getenv("RAW_SCHEMA_PATH")

with open(RAW_SCHEMA_PATH, "r") as f:
    data_schema = json.load(f)


def download_live_weather_data(spark) -> None:
    """Download live weather data for specified cities using Spark DataFrame."""
    logger.info("Starting downloading script.")
    try:
        request_df = assemble_request_df(spark)
        udf_executeRestApi = udf(executeRestApiAndSave, REQUEST_SCHEMA)
        result_df = request_df.withColumn(
            "result", udf_executeRestApi(col("method"), col("city"))
        )
        df = result_df.select(explode(col("result")).alias("results"))
        df.select(
            collapse_columns(df.schema)
        ).collect()  # collect is faster but should not be
        # used for very large datasets
        logger.info("Downloading script finished succesfully.")

    except Exception as e:
        logger.error(e)


def executeRestApiAndSave(method: str, city: str) -> None:
    """Execute REST API request and save data."""

    data = request_from_weather_api(method, city)
    validate_schema(data, method)
    storage_path = f"{RAW_ROOT_PATH}/{method}"
    path = check_path(storage_path, DATE)
    persist_on_storage(data, city, path, method)


def validate_schema(data: dict, method: dict):
    """
    Check if downloaded data's schema remains the same, warning
    the user otherwise and interrupting the processing
    """
    try:
        validate(instance=data, schema=data_schema[method])
    except Exception as e:
        logger.error("There was error while validating the schema.")
        logger.error(f"Error message:{e}")


def persist_on_storage(
    data: Dict[str, str], city: str, folder_path: str, method: str
) -> None:
    """Persist data on storage."""
    file_path = f"{folder_path}/{city}.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
        logger.info(f'{city} "{method}" data saved.')


def assemble_request_df(spark) -> List[Tuple[str, str]]:
    """Assemble rows for REST API requests."""
    with open(CITIES_JSON_PATH, "r") as f:
        cities = json.load(f)
    RestApiRequestRow = Row("method", "city")
    RestApiRequestRows = [
        RestApiRequestRow("current", city) for city in cities.values()
    ]
    RestApiRequestRows += [
        RestApiRequestRow("sports", city) for city in cities.values()
    ]
    RestApiRequestRows += [
        RestApiRequestRow("astronomy", city) for city in cities.values()
    ]
    RestApiRequestRows += [
        RestApiRequestRow("marine", city) for city in cities.values()
    ]

    request_df = spark.createDataFrame(RestApiRequestRows)

    return request_df


def collapse_columns(source_schema: StructType, columnFilter: str = None) -> List:
    """Collapse columns based on the specified filter."""
    _columns_to_select = []
    if columnFilter is None:
        columnFilter = ""
    _all_columns = get_all_columns_from_schema(source_schema)
    for column_collection in _all_columns:
        if (len(columnFilter) > 0) & (column_collection[0] != columnFilter):
            continue

        select_column_collection = [
            "`%s`" % list_item for list_item in column_collection
        ]

        if len(column_collection) > 1:
            _columns_to_select.append(
                col(".".join(select_column_collection)).alias(
                    "_".join(column_collection)
                )
            )
        else:
            _columns_to_select.append(col(select_column_collection[0]))

    return _columns_to_select


def get_all_columns_from_schema(source_schema: StructType) -> List[List[str]]:
    """Get all columns from the schema."""
    branches = []

    def inner_get(schema, ancestor=None):
        if ancestor is None:
            ancestor = []
        for field in schema.fields:
            branch_path = ancestor + [field.name]
            if isinstance(field.dataType, StructType):
                inner_get(field.dataType, branch_path)
            else:
                branches.append(branch_path)

    inner_get(source_schema)

    return branches
