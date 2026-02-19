import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

db_config = {
    'host': 'localhost',
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_NAME")
}

def getdbconn():
    return mysql.connector.connect(**db_config)
