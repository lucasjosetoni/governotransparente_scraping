import hashlib
import os
import re
import unicodedata

import psycopg2


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "transparencia"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
    )


def clean_supplier_name(value):
    text = (value or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_for_compare(value):
    text = clean_supplier_name(value).upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Z0-9 ]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def hash_supplier(value):
    normalized = normalize_for_compare(value)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def only_digits(value):
    return re.sub(r"\D", "", value or "")
