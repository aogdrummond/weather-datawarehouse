import os
import requests
from dotenv import load_dotenv
from dags.src.logger_config import setup_logging

load_dotenv()

logger = setup_logging(__name__)
logger.propagate = False

WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
WEATHER_BASE_URL = os.getenv("WEATHER_BASE_URL")
COUNTRY_API_ROOT_URL = os.getenv("COUNTRY_API_ROOT_URL")


def request_from_weather_api(method:str, city:str)->dict:
    """
    Make a request to the weather API to retrieve data for a specified location.

    Parameters:
    - method (str): The type of weather data to request (e.g., 'current', 'marine').
    - city (str): The name of the city for which weather data is requested.

    Returns:
    - dict: The weather data for the specified city in JSON format. Returns None if an error occurs.
    """
    REQUEST_URL = f"{method}.json?key={WEATHER_API_KEY}&q={city}"
    FINAL_URL = WEATHER_BASE_URL + REQUEST_URL
    response = requests.get(url=FINAL_URL)

    if response.status_code != 200:
        logger.error(f"Error requesting {city} {method} data.")
        return

    return response.json()


def request_country_data(country:str)->dict:
    """
    Make a request to countries API to retrieve data for a specified country.

    Parameters:
    - country (str): The name of the country for which data is requested.

    Returns:
    - dict: Filtered country data containing information about the continent, timezone, name,
            region, language, and capital of the specified country. Returns None if an error occurs.
    """
    url = f"{COUNTRY_API_ROOT_URL}/{country}?fullText=true"
    response = requests.get(url)
    if response.status_code == 200:
        complete_country_data = response.json()
        logger.info(f"Country data for country '{country}' obtained from the API.")
        filtered_country_data = filter_country_data(complete_country_data)
    else:
        filtered_country_data = None
    return filtered_country_data


def filter_country_data(data: dict) -> dict:
    """
    Filter relevant information from raw country data obtained from an API,
    processing the string data when required.

    Parameters:
    - data (dict): Raw country data obtained from an API.

    Returns:
    - dict: Filtered country data containing information about the continent, timezone, name,
            region, language, and capital.
    """
    filtered_data = {
        "continent": data[0].get("continents",[''])[0].replace("'", ""),
        "tz_utc": data[0].get("timezones",[''])[0].replace("'", "").replace("UTC",""),
        "name": data[0].get("name",{}).get("common",'').replace("'", ""),
        "region": data[0].get("subregion",'').replace("'", ""),
        "language": str(list(data[0].get("languages").values())).replace("'", "").replace("[", "").replace("]", ""),
        "capital": data[0].get("capital",[''])[0].replace("'", ""),
    }
    return filtered_data

