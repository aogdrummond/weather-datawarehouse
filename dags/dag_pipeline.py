import os
import sys
current_directory = os.path.abspath(os.path.dirname(__name__))
sys.path.append(current_directory)
from dotenv import load_dotenv

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import  SparkSession
from dags.src.spark_download_raw_data import download_live_weather_data
from dags.src.spark_transform_data import transform_raw_data
from dags.src.spark_insert_data import insert_data_in_db
load_dotenv()

JDBC_DRIVER_PATH = os.getenv('JDBC_DRIVER_PATH')
spark = SparkSession.builder.appName('Spark Data Warehouse app').config('spark.jars',JDBC_DRIVER_PATH).getOrCreate()

# Define the functions to be executed by the PythonOperators
def data_download():
    download_live_weather_data(spark)

def data_transformation():
    transform_raw_data(spark)

def data_insertion():
    insert_data_in_db(spark)
    
# Define default arguments for the DAG
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 10,20,25,0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create a DAG object
dag = DAG('complete_pipeline_execution',
          default_args=default_args,
          schedule='*/5 * * * *'
          ) 
# Define PythonOperators that will execute the Python scripts sequentially
download_task = PythonOperator(
    task_id='execute_script1',
    python_callable=data_download,  # Specify the function to be executed
    dag=dag,
)

transformation_task = PythonOperator(
    task_id='execute_script2',
    python_callable=data_transformation,  # Specify the function to be executed
    dag=dag,
)

insertion_task = PythonOperator(
    task_id='execute_script3',
    python_callable=data_insertion,  # Specify the function to be executed
    dag=dag,
)

# Define the execution sequence using task dependencies
download_task >> transformation_task >> insertion_task