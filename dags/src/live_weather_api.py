from dotenv import load_dotenv
from datetime import datetime
from jsonschema import validate
import logging
import requests
import json
import os

#inserir logs

load_dotenv()
API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv('BASE_URL')
DATA_SCHEMA_PATH = "weather_schema.json"

with open(DATA_SCHEMA_PATH,'r') as f:
    DATA_SCHEMA = json.load(f)

def download_live_weather_data():
    try:
        requested_cities_path = os.getenv('CITIES_JSON_PATH')
        methods = ['current','sports','astronomy','marine']
        with open(requested_cities_path,'r') as f:
            cities = json.load(f)
        [extract_raw_dataset(method,cities) for method in methods]
    except Exception as e:
        print(e)

def extract_raw_dataset(method:str,cities:dict):
    """
    """
    storage_path = f'storage/{method}'
    check_required_folder(storage_path)
    day_path = storage_path + f'/{datetime.now().strftime("%Y%m%d%H%M")}'
    check_required_folder(day_path)
    
    for country,city in cities.items():
        response = request_weather_data(method,city)
        if (response.status_code < 200) or (response.status_code >= 300):
            logging.error(f"""Error on request ({country}/{city}). Status code: {response.status_code}""")
            
        else:
            data = response.json()
            validate_schema(data,method)
            persist_on_storage(data=data,
                                city=city,
                                folder_path=day_path)

def request_weather_data(method:str,city:str):
        
    REQUEST_URL = f'{method}.json?key={API_KEY}&q={city}'
    FINAL_URL = BASE_URL + REQUEST_URL
    response = requests.get(url=FINAL_URL)
    return response

def validate_schema(data:dict,method:dict):
    """"""
    try:
        if method == 'current':
            validate(instance=data,schema=DATA_SCHEMA)
    except Exception as e:
        #INSERIR UMA TAG NO AIRFLOW PARA FLUXO NÃO CONTINUAR SE ISSO ACONTECER
        logging.error('There was error while validating the schema.')
        logging.error(f'Error message:{e}')
        

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
