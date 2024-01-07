import os
import psycopg2

conn = psycopg2.connect(database=os.getenv('DB_NAME'),
                        host=os.getenv('DB_HOST'),
                        user=os.getenv('DB_USER'),
                        password=os.getenv('DB_PASSWORD'),
                        port=os.getenv('DB_PORT'))

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
        self.url = os.getenv('DB_URL')
        self.driver = os.getenv('DB_DRIVER')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')

    def fetch_location(self,city,lat,long):

        if lat is None:
            lat = "NULL"
        if long is None:
            long = "NULL"
        query = "SELECT id FROM locations WHERE (city = '{}') OR (lat = {} AND long = {});".format(city,lat,long)
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        if len(result) > 0:
            return result[0][0]
        else:
            return []

    def commit(self):
        """
        Commits operation's data to the database.
        """
        self.commit()