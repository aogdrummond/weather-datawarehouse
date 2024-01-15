import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

conn = psycopg2.connect(
    database=os.getenv("DB_NAME"),
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT"),
)

cursor = conn.cursor()


class DbCursor:
    """
    A class for interacting with a MySQL database.

    Attributes:
        cursor (MySQLCursor): The MySQL cursor object for executing queries.
    """

    def __init__(self) -> None:
        """
        Initializes a DB_Cursor object with a given MySQL cursor.

        Args:
            cursor (MySQLCursor): The MySQL cursor object for executing queries.
        """
        self.cursor = cursor
        self.url = os.getenv("DB_URL")
        self.driver = os.getenv("DB_DRIVER")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")

    def fetch_location(self, city, lat, long):
        if lat is None:
            lat = "NULL"
        if long is None:
            long = "NULL"
        query = "SELECT id FROM locations WHERE (city = '{}') OR (lat = {} AND long = {});".format(
            city, lat, long
        )
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        if len(result) > 0:
            return result[0][0]
        else:
            return []

    def fetch_country(self, country):
        query = "SELECT id FROM countries WHERE name = '{}' ;".format(country)
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        if len(result) > 0:
            return result[0][0]
        else:
            return None

    def insert_country(self, country_data):
        created_at = datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S")
        query = "INSERT INTO countries (name,capital,language,tz_utc,region,\
            continent,created_at) VALUES ('{}','{}','{}','{}','{}','{}','{}');".format(
            country_data["name"],
            country_data["capital"],
            country_data["language"],
            country_data["tz_utc"],
            country_data["region"],
            country_data["continent"],
            created_at,
        )
        self.cursor.execute(query)
        self.commit()

    def commit(self):
        """
        Commits operation's data to the database.
        """
        conn.commit()


if __name__ == "__main__":
    dbcursor = DbCursor()
    country_data = {
        "name": "nome",
        "capital": "capital",
        "language": "language",
        "tz_utc": "tz",
        "region": "region",
        "continent": "continent",
    }
    response = dbcursor.fetch_country("Dominican Republic")
    print()
