import os
import json
import requests
from datetime import datetime
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import Row, SparkSession

# spark = SparkSession.builder.appName('Spark download app').config('','').getOrCreate()

API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv('BASE_URL')
CITIES_JSON_PATH = os.getenv('CITIES_JSON_PATH')

def download_live_weather_data(spark):
  try:
    schema = ArrayType(StructType([StructField("method",StringType(),False),
                     StructField("city",StringType(),False),
                     StructField("result",StructType(),True)]))

    udf_executeRestApi = udf(executeRestApiAndSave, schema)
    RestApiRequestRows = assemble_request_rows()
    request_df = spark.createDataFrame(RestApiRequestRows)
    result_df = request_df.withColumn("result", udf_executeRestApi(col("method"), col("city")))
    df = result_df.select(explode(col("result")).alias("results"))
    df.select(collapse_columns(df.schema)).show()
  except Exception as e:
    #Criar tratamento de exceção
    print(e)

def executeRestApiAndSave(method,city):

    REQUEST_URL = f'{method}.json?key={API_KEY}&q={city}'
    FINAL_URL = BASE_URL + REQUEST_URL
    response = requests.get(url=FINAL_URL)
    if response.status_code != 200:
        print('DEU BIZIU!')
    data = response.json()
    storage_path = f'storage/raw/{method}'
    check_required_folder(storage_path)
    path = storage_path + f'/{datetime.now().strftime("%Y%m%d")}'
    check_required_folder(path)
    persist_on_storage(data,city,path)
    return None

def check_required_folder(path):
    if not os.path.isdir(path):
        os.mkdir(path)
        print(f'Folder {path} created.')

def persist_on_storage(data:dict,
                       city:str,
                       folder_path:str):
    file_path = folder_path+f'/{city}.json'
    with open(file_path,'w') as f:
        json.dump(data,f)
        print(f'{city} data saved.')

def assemble_request_rows():

  with open(CITIES_JSON_PATH,'r') as f:
    cities = json.load(f)
  RestApiRequestRow = Row("method", "city")
  RestApiRequestRows = [RestApiRequestRow("current", city) for city in cities.values()]
  RestApiRequestRows += [RestApiRequestRow("sports", city) for city in cities.values()]
  RestApiRequestRows += [RestApiRequestRow("astronomy", city) for city in cities.values()]
  RestApiRequestRows += [RestApiRequestRow("marine", city) for city in cities.values()]
  
  return RestApiRequestRows

def collapse_columns(source_schema, columnFilter=None):
  _columns_to_select = []
  if columnFilter is None: columnFilter = ""
  _all_columns = get_all_columns_from_schema(source_schema)
  for column_collection in _all_columns:
    if (len(columnFilter) > 0) & (column_collection[0] != columnFilter): 
        continue

    select_column_collection = ['`%s`' % list_item for list_item in column_collection]    
    
    if len(column_collection) > 1:
      _columns_to_select.append(col('.'.join(select_column_collection)).alias('_'.join(column_collection)))
    else:
      _columns_to_select.append(col(select_column_collection[0]))

  return _columns_to_select

def get_all_columns_from_schema(source_schema):
  
  branches = []
  def inner_get(schema, ancestor=None):
    if ancestor is None: ancestor = []
    for field in schema.fields:
      branch_path = ancestor+[field.name]     
      if isinstance(field.dataType, StructType):    
        inner_get(field.dataType, branch_path) 
      else:
        branches.append(branch_path)
        
  inner_get(source_schema)
        
  return branches
