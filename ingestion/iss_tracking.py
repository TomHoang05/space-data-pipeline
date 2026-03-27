import os
import time
import psycopg2
import requests
import logging
from tenacity import retry, wait_exponential, stop_after_attempt, before_sleep_log, retry_if_not_exception_type
from dotenv import load_dotenv

# load .env variables into memory
load_dotenv()

# logging settings
logger = logging.getLogger(__name__)
handler1 = logging.StreamHandler()
handler2 = logging.FileHandler(filename = 'logs/iss_tracking.log')

logger.addHandler(handler1)
logger.addHandler(handler2)

logger.setLevel(logging.INFO)

def create_tbl(conn, cur):
    # create table in postgres if not exists
    cur.execute(""" 
        CREATE TABLE IF NOT EXISTS iss_position (
            id SERIAL PRIMARY KEY,
            longitude FLOAT,
            latitude FLOAT,
            timestamp INT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """ 
    )
    conn.commit()

# retry logic
@retry(stop = stop_after_attempt(5), wait = wait_exponential(multiplier = 1, min = 4, max = 10), before_sleep = before_sleep_log(logger, logging.WARNING), retry = retry_if_not_exception_type(KeyError))
def extraction():
    #fetch data from api
    response = requests.get("http://api.open-notify.org/iss-now.json")
    response.raise_for_status()
    data = response.json()

    # Key validation
    if 'timestamp' not in data or 'iss_position' not in data or 'longitude' not in data['iss_position'] or 'latitude' not in data['iss_position']:
        raise KeyError("Invalid Keys")
    else:
        longitude = data['iss_position']['longitude']
        latitude = data['iss_position']['latitude']
        timestamp = data['timestamp']

        return longitude, latitude, timestamp

def insertion(conn, cur, long, lat, tp):
    # load data into table
        cur.execute(""" 
            INSERT INTO iss_position (longitude, latitude, timestamp)
            VALUES (%s, %s, %s)
            """, (long, lat, tp)
        )
        conn.commit()
        logger.info("Data Recorded")

if __name__ == "__main__":
    # variable initialization
    connection = None
    cursor = None

    # connect to postgres using .env variables
    try:
        logger.info('Started')
        connection = psycopg2.connect(
            host = os.getenv('DB_HOST'),
            database = os.getenv('DB_NAME'),
            user = os.getenv('DB_USER'),
            password = os.getenv('DB_PASSWORD'),
            port = os.getenv('DB_PORT')
        )
        cursor = connection.cursor()
        logger.info("Connected to Postgres")

        #run ingestion
        create_tbl(connection, cursor)

        for i in range(5):
            long, lat, tp = extraction()
            insertion(connection, cursor, long, lat, tp)
            time.sleep(10)
        logger.info('Finished')

    except Exception as e:
        logger.critical(f"Error: {e}")

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()