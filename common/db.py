import psycopg2
import os

def get_connection():
    # Connect to postgres using .env variables
    connection = psycopg2.connect(
        host = os.getenv('DB_HOST'),
        database = os.getenv('DB_NAME'),
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASSWORD'),
        port = os.getenv('DB_PORT')
    )
    cursor = connection.cursor()

    return connection, cursor
    
def close_connection(connection, cursor):
    if cursor is not None:
        cursor.close()

    if connection is not None:
        connection.close()