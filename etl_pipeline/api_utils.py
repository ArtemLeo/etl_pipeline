import requests
from etl_pipeline.config import API_BASE_URL, CLIENT_ID, CLIENT_SECRET


def get_data_from_api(endpoint, start_date, page=0, limit=10000):
    """
    Отримати дані з API.

    :param endpoint: Ендпоінт API, до якого потрібно зробити запит
    :param start_date: дата початку для фільтрації даних
    :param page: номер сторінки для пагінації
    :param limit: кількість записів на сторінці
    :return: JSON-дані з API, якщо запит успішний
    :raises Exception: у випадку невдачі запиту
    """
    url = f"{API_BASE_URL}{endpoint}?page={page}&limit={limit}&date_time_start={start_date}"
    headers = {
        "CF-Access-Client-Id": CLIENT_ID,
        "CF-Access-Client-Secret": CLIENT_SECRET
    }
    print(f"Requesting URL: {url}")
    response = requests.get(url, headers=headers)
    print(f"Response Status Code: {response.status_code}")
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Response Text: {response.text}")
        raise Exception(f"Failed to get data: {response.status_code}, {response.text}")
