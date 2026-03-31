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
    text = text.replace("\ufffd", " ")
    # Remove sequencias de escape quebradas e variações incompletas.
    text = re.sub(r"\\[Uu][0-9a-fA-F]{0,8}", " ", text)
    text = re.sub(r"\\[xX][0-9a-fA-F]{0,2}", " ", text)
    text = re.sub(r"\\N\{[^}]*\}", " ", text)
    text = text.replace("\\", " ")
    # Remove caracteres de controle que podem poluir prompt/output.
    text = re.sub(r"[\x00-\x1f\x7f]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def sanitize_for_llm(value):
    # Versao agressiva para prompt de LLM: limpa escapes e gera texto canonico.
    text = clean_supplier_name(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Za-z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
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
