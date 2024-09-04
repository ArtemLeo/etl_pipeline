import psycopg2
from datetime import datetime
import pytz
from etl_pipeline.config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def connect_to_db():
    """
    Під'єднатися до бази даних PostgreSQL.

    :return: З'єднання з базою даних або None у випадку помилки
    """
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


def create_full_api_data_table(connection):
    """
    Створити таблицю для збереження всіх даних з API.

    :param connection: З'єднання з базою даних
    """
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS full_api_data (
            id SERIAL PRIMARY KEY,
            company_id INTEGER,
            company_name VARCHAR(255),
            employee_id INTEGER,
            employee_ipn VARCHAR(20),
            employee_full_name VARCHAR(255),
            work_name VARCHAR(255),
            work_code VARCHAR(255),
            job_start TIMESTAMP,
            job_end TIMESTAMP,
            work_order_id INTEGER,
            work_order_number VARCHAR(255)
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        print("Full API Data table created successfully")
    except Exception as e:
        print(f"Failed to create Full API Data table: {e}")


def save_full_api_data_to_db(connection, data):
    """
    Зберегти всі дані з API у базі даних.

    :param connection: З'єднання з базою даних
    :param data: дані, отримані з API
    """
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO full_api_data (
            company_id, company_name, employee_id, employee_ipn, employee_full_name,
            work_name, work_code, job_start, job_end, work_order_id, work_order_number
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for record in data:
            company_id = record.get("CompanyId")
            company_name = record.get("CompanyName")
            employee_id = record.get("EmployeeId")
            employee_ipn = record.get("EmployeeIpn")
            employee_full_name = record.get("EmployeeFullName")
            work_name = record.get("workname")
            work_code = record.get("workcode")
            job_start = record.get("jobstart")
            job_end = record.get("jobend")
            work_order_id = record.get("workorderid")
            work_order_number = record.get("workordernumber")

            job_start = convert_utc_to_local(job_start)
            job_end = convert_utc_to_local(job_end)

            cursor.execute(insert_query, (
                company_id, company_name, employee_id, employee_ipn, employee_full_name,
                work_name, work_code, job_start, job_end, work_order_id, work_order_number
            ))
        connection.commit()
        cursor.close()
        print("Data saved to database successfully")
    except Exception as e:
        print(f"Failed to save data to database: {e}")


def convert_utc_to_local(utc_dt_str):
    """
    Перетворити UTC дату і час в локальний час.

    :param utc_dt_str: Дата і час в UTC
    :return: локальна дата і час
    """
    if utc_dt_str:
        try:
            utc_dt = datetime.strptime(utc_dt_str, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            utc_dt = datetime.strptime(utc_dt_str, "%Y-%m-%dT%H:%M:%S")
        utc_dt = utc_dt.replace(tzinfo=pytz.utc)
        local_dt = utc_dt.astimezone(pytz.timezone("Europe/Kiev"))
        return local_dt
    return None
