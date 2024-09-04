"""
В цьому файлі зберігатимуться конфігураційні дані:
- CF-Access-Client-Id,
- CF-Access-Client-Secret,
- базова URL-адреса API
"""

import os
from dotenv import load_dotenv

# Завантаження змінних середовища з файлу .env
load_dotenv()

# API Configurations
API_BASE_URL = os.getenv("API_BASE_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Database Configurations
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
