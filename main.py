# Для запуску ETL процесу


from etl_pipeline import connect_to_db

if __name__ == "__main__":
    db_connection = connect_to_db()
    if db_connection:
        db_connection.close()
