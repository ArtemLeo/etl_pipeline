from etl_pipeline.api_utils import get_data_from_api
from etl_pipeline.db_utils import connect_to_db, create_full_api_data_table, save_full_api_data_to_db
from etl_pipeline.config import API_BASE_URL


def get_all_data_from_api(endpoint, start_date, limit=10000):
    """
    Отримати всі дані з API, обробляючи всі сторінки.

    :param endpoint: Ендпоінт API для отримання даних
    :param start_date: дата початку для фільтрації даних
    :param limit: кількість записів на сторінці
    :return: Список всіх даних з API
    """
    all_data = []
    page = 0
    while True:
        data = get_data_from_api(endpoint, start_date, page=page, limit=limit)
        if not data:
            break
        all_data.extend(data)
        page += 1
    return all_data


def main():
    """
    Основна функція для запуску ETL процесу.

    - Отримує дані з API
    - Підключається до бази даних
    - Створює таблицю, якщо її ще не існує
    - Зберігає дані в базі даних
    """
    # Отримати всі дані з API
    start_date = "2024-07-01T00:00:00"
    endpoint = "works_duration"  # Замініть на потрібний ендпоінт

    data = get_all_data_from_api(endpoint, start_date)

    if data:
        # Під'єднатися до бази даних
        connection = connect_to_db()
        if connection:
            # Створити таблицю full_api_data
            create_full_api_data_table(connection)
            # Зберегти дані у базі даних
            save_full_api_data_to_db(connection, data)
            # Закрити з'єднання з базою даних
            connection.close()


if __name__ == "__main__":
    # Запуск основної функції ETL процесу
    main()
