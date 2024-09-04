# В цьому файлі реалізована основна логіка ETL пайплайну.

import requests
import psycopg2
from config import API_BASE_URL, CLIENT_ID, CLIENT_SECRET, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def get_data_from_api(endpoint, start_date, page=0, limit=10000):
    url = f"{API_BASE_URL}{endpoint}?page={page}&limit={limit}&date_time_start={start_date}"
    headers = {
        "CF-Access-Client-Id": CLIENT_ID,
        "CF-Access-Client-Secret": CLIENT_SECRET
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to get data: {response.status_code}, {response.text}")


# Функція для підключення до бази даних
def connect_to_db():
    try:
        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Connected to the database")
        return connection
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return None


# Приклад використання функції
if __name__ == "__main__":
    start_date = "2024-07-01T00:00:00"
    data = get_data_from_api("works_duration/", start_date)
    print(data)
