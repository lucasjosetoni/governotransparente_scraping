import csv
import json
import os
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor


SELECT_FORNECEDORES_SQL = """
    SELECT *
    FROM dw.fornecedores_enriquecidos
    WHERE fornecedor_normalizado IS NOT NULL
    ORDER BY atualizado_em DESC NULLS LAST, id_fornecedor DESC
"""


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "transparencia"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
    )


def ensure_output_dir():
    out_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def write_json(rows, out_path):
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2, default=str)


def write_csv(rows, out_path):
    if not rows:
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    headers = list(rows[0].keys())
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def main():
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(SELECT_FORNECEDORES_SQL)
            rows = [dict(row) for row in cur.fetchall()]

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = ensure_output_dir()

        json_path = os.path.join(out_dir, f"fornecedores_enriquecidos_{timestamp}.json")
        csv_path = os.path.join(out_dir, f"fornecedores_enriquecidos_{timestamp}.csv")

        write_json(rows, json_path)
        write_csv(rows, csv_path)

        print(f"Total de fornecedores enriquecidos: {len(rows)}")
        print(f"JSON: {json_path}")
        print(f"CSV: {csv_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
