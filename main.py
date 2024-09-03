# Для запуску ETL процесу


from etl_pipeline import get_data_from_api

if __name__ == "__main__":
    start_date = "2024-07-01T00:00:00"
    data = get_data_from_api("works_duration/", start_date)
    print(data)
