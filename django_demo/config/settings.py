"""Deliberately small, localhost-only workshop settings; not deployment settings."""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SECRET_KEY = "local-kafka-workshop-not-a-production-secret"
DEBUG = True
ALLOWED_HOSTS = ["localhost", "127.0.0.1", "[::1]", "testserver"]
INSTALLED_APPS = ["orders"]
MIDDLEWARE = [
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
]
ROOT_URLCONF = "config.urls"
DATABASES = {"default": {
    "ENGINE": "django.db.backends.sqlite3",
    "NAME": os.environ.get("DJANGO_DB_PATH", str(BASE_DIR / "db.sqlite3")),
    "OPTIONS": {"timeout": 20},
}}
USE_TZ = True
TIME_ZONE = "UTC"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
KAFKA_ORDERS_TOPIC = os.environ.get("KAFKA_ORDERS_TOPIC", "lab.django.orders")
DATA_UPLOAD_MAX_MEMORY_SIZE = 64 * 1024
