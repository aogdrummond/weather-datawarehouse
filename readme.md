# Weather by Location Data Warehouse

This project is being developed aiming to apply several current concepts and tools for Data Warehouse, expecting to generate value given the suggested source. Those tools include(but are not limited to): **Python**, **Pandas**, **PySpark**, **Apache Airflow**, **Database Design** and **AWS**.

It is contemplated all the main components expected for an ETL process: raw data extraction from the source (in this case, consumption from an external API), data transformation and insertion to the data warehouse, aiming to obtain the called **Data Lakehouse**, given that it is expected to get positive features from both data warehouse and data lake models.

## Overview

Explain the project's architecture, mentioning key components and their interactions:

- **Data Source**: Raw weather data obtained from the free api `https://www.weatherapi.com/` .
- **Data Processing**: All the data processing workflow developed using PySpark.
- **Database**: Local/AWS storage for raw and processed data and Postgres for the final data.
- **Orchestration**: Schedule and data flow orchestration managed by Airflow.

## Requirements [TO-DO]

- List the necessary dependencies and tools needed to run the project, including versions.
  - Python (version)
  - PySpark (version)
  - Airflow (version)
  - Any additional libraries or packages

## Setup [TO-DO]

Provide instructions for setting up the project:

1. **Clone the Repository**: `git clone [repository_url]`
2. **Install Dependencies**: `pip install -r requirements.txt`
3. **Configuration**: Explain how to set up configuration files for API keys, database connection strings, etc.

## Usage [TO-DO]

Explain how to run the project:

1. **Data Fetching**: Steps to fetch data from the API(s).
2. **Data Processing**: Running PySpark scripts for data transformation.
3. **Database Insertion**: How to insert processed data into the SQL database.
4. **Airflow Execution**: Instructions to start the Airflow DAG(s) for orchestration.

## File Structure

Outline the structure of the project's files and directories:
```
weather-datawarehouse/
│
├── __main__.py
|
├── .env
|
├── .gitignore
|
├── requirements.txt
|
├── readme.md
|
|
├── storage/
│   ├── raw/
│   └── processed/
│
├── src/
│   ├── live_weather_api.py
│   ├── spark_download_raw_data.py
│   ├── spark_transform_data.py
│   └── logger_config.py
│
├── schemas/
│   ├── attributes_mapping.py
│   ├── cities.json
│   └── weather_schema.json
|
├── log/
│   └── app.log
|
├── exploratory_analysis/
│   └── exploratory_analysis_with_pandas.ipynb
│
└── airflow/
    └── dags/
        └── airflow_dag.py
```

# Examples [TO-DO]

Provide code snippets or examples for executing specific functionalities within the project.

### Fetching Data from API

```python
# Example code for fetching data from the API
# ...
```
### PySpark Data Processing

```python
# Example code for fetching data from the API
# ...
```

### Database Insertion


```python
# Example code for fetching data from the API
# ...
```
### Airflow DAG Configuration

```python
# Example code for fetching data from the API
# ...
```
## License


