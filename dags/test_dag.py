import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

DATE = str(datetime.now().date())
# Define the functions to be executed by the PythonOperators
def function1():
    print("Executing script1.py...")
    path = f'/home/cliente/Projects/weather-datawarehouse/dags/{DATE}.json'
    with open(path,'w') as f:
        json.dump([1],f)

def function2():
    print("Executing script2.py...")
    path = f'/home/cliente/Projects/weather-datawarehouse/dags/{DATE}.json'
    with open(path,'r') as f:
        lista = json.load(f)
    lista.append(2)
    with open(path,'w') as f:
        json.dump(lista,f)
    

def function3():
    print("Executing script3.py...")
    path = f'/home/cliente/Projects/weather-datawarehouse/dags/{DATE}.json'
    with open(path,'r') as f:
        lista = json.load(f)
    lista.append(3)
    with open(path,'w') as f:
        json.dump(lista,f)

# Define default arguments for the DAG
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    # 'start_date': datetime(2024, 1, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG object
dag = DAG('execute_multiple_python_scripts',
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