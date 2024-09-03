# В цьому файлі реалізована основна логіка ETL пайплайну.

import requests
from config import API_BASE_URL, CLIENT_ID, CLIENT_SECRET


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


# Приклад використання функції
if __name__ == "__main__":
    start_date = "2024-07-01T00:00:00"
    data = get_data_from_api("works_duration/", start_date)
    print(data)