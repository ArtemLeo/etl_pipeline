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

API_BASE_URL = os.getenv("API_BASE_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
