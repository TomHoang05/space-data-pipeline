import sys
import os

# look for modules from project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import time
import requests
import logging
import yaml
import json
import datetime
from common.db import get_connection, close_connection
from tenacity import retry, wait_exponential, stop_after_attempt, before_sleep_log, retry_if_not_exception_type
from dotenv import load_dotenv

# load .env variables into memory
load_dotenv()

# load config file
with open(os.path.join(os.path.dirname(__file__), 'config.yaml'), 'r') as file:
    config = yaml.safe_load(file)

# load schema
with open(os.path.join(os.path.dirname(__file__), '..', '..', config['schema']['iss_position']), 'r') as table:
    sql = table.read()

# logging settings
logger = logging.getLogger(__name__)
handler1 = logging.StreamHandler()
handler2 = logging.FileHandler(filename = os.path.join(os.path.dirname(__file__), '..', '..', config['logging']['path']))

logger.addHandler(handler1)
logger.addHandler(handler2)

logger.setLevel(logging.INFO)

# create table in postgres if not exists
def create_tbl(conn, cur):
    cur.execute(sql)
    conn.commit()

# retry logic, data extraction
@retry(stop = stop_after_attempt(5), wait = wait_exponential(multiplier = 1, min = 4, max = 10), before_sleep = before_sleep_log(logger, logging.WARNING), retry = retry_if_not_exception_type(KeyError))
def extraction():
    #fetch data from api
    response = requests.get(config['api']['url'])
    response.raise_for_status()
    data = response.json()

    # Key validation
    if 'timestamp' not in data or 'iss_position' not in data or 'longitude' not in data['iss_position'] or 'latitude' not in data['iss_position']:
        raise KeyError("Invalid Keys")
    else:
        longitude = data['iss_position']['longitude']
        latitude = data['iss_position']['latitude']
        timestamp = data['timestamp']

        return data, longitude, latitude, timestamp

# record raw data   
def save_raw(data):
    curr_time = datetime.date.today()
    time_string = curr_time.strftime("%Y-%m-%d")
    raw_json = json.dumps(data)
    with open (os.path.join(os.path.dirname(__file__), '..', '..', config['raw']['path'], time_string + ".jsonl"), 'a') as file:
        file.write(raw_json + "\n")

# load data into table  
def load(conn, cur, long, lat, tp):
        cur.execute(""" 
            INSERT INTO iss_position (longitude, latitude, timestamp)
            VALUES (%s, %s, %s)
            ON CONFLICT (longitude, latitude, timestamp)
            DO NOTHING
            """, (long, lat, tp)
        )
        conn.commit()
        logger.info("Data Recorded")

if __name__ == "__main__":
    # variable initialization
    connection = None
    cursor = None

    try:
        # connect to postgres
        logger.info('Started')
        connection, cursor = get_connection()
        logger.info("Connected to Postgres")

        # run ingestion
        create_tbl(connection, cursor)

        for i in range(config['loop']['count']):
            data, long, lat, tp = extraction()
            save_raw(data)
            load(connection, cursor, long, lat, tp)
            time.sleep(config['loop']['delay'])
        logger.info('Finished')

    except Exception as e:
        logger.critical(f"Error: {e}")

    finally:
        close_connection(connection, cursor)