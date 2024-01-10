import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import  SparkSession
from src.spark_download_raw_data import download_live_weather_data
from src.spark_transform_data import transform_raw_data
from src.spark_insert_data import insert_data_in_db
load_dotenv()

JDBC_DRIVER_PATH = os.getenv('JDBC_DRIVER_PATH')
spark = SparkSession.builder.appName('Spark Data Warehouse app').config('spark.jars',JDBC_DRIVER_PATH).getOrCreate()

DATE = str(datetime.now().date())
# Define the functions to be executed by the PythonOperators
def function1():
    download_live_weather_data(spark)

def function2():
    transform_raw_data(spark)

def function3():
    insert_data_in_db(spark)
    
# Define default arguments for the DAG
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    # 'start_date': datetime(2024, 1, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG object
dag = DAG('zubumafu',
          default_args=default_args,
        #   schedule=timedelta(minutes=1)
          )  # Run the DAG daily

# Define PythonOperators that will execute the Python scripts sequentially
execute_script1 = PythonOperator(
    task_id='execute_script1',
    python_callable=function1,  # Specify the function to be executed
    dag=dag,
)

execute_script2 = PythonOperator(
    task_id='execute_script2',
    python_callable=function2,  # Specify the function to be executed
    dag=dag,
)

execute_script3 = PythonOperator(
    task_id='execute_script3',
    python_callable=function3,  # Specify the function to be executed
    dag=dag,
)

# Define the execution sequence using task dependencies
execute_script1 >> execute_script2 >> execute_script3