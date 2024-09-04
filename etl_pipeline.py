# В цьому файлі реалізована основна логіка ETL пайплайну.

import os
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


# Приклад створення таблиці для workers
def create_tables(connection):
    try:
        cursor = connection.cursor()

        # SQL-запит для створення таблиці workers
        create_workers_table = """
        CREATE TABLE IF NOT EXISTS workers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            department VARCHAR(255)
        );
        """
        cursor.execute(create_workers_table)
        connection.commit()
        cursor.close()
        print("Tables created successfully")
    except Exception as e:
        print(f"Failed to create tables: {e}")


# Функція для отримання даних з API
def fetch_data_from_api(url, params=None):
    headers = {
        "CF-Access-Client-Id": os.getenv("CLIENT_ID"),
        "CF-Access-Client-Secret": os.getenv("CLIENT_SECRET")
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data: {e}")
        return None


if __name__ == "__main__":
    # Отримуємо URL з .env файлу
    url = os.getenv("API_WORKS_DURATION_URL")
    params = {
        "page": 0,
        "limit": 10000,
        "date_time_start": "2024-07-01T00:00:00"
    }

    data = fetch_data_from_api(url, params)

    if data:
        print("Data fetched successfully")
        # Тут можна додати код для обробки та збереження даних в базу
