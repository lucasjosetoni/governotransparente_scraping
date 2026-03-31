from common import clean_supplier_name, get_db_conn, hash_supplier


SELECT_SQL = """
    SELECT DISTINCT fornecedor
    FROM dw.empenhos
    WHERE fornecedor IS NOT NULL
      AND BTRIM(fornecedor) <> ''
"""

UPSERT_SQL = """
    INSERT INTO raw.fornecedores_unicos (
        fornecedor_original,
        fornecedor_limpo,
        hash_fornecedor,
        atualizado_em
    )
    VALUES (%s, %s, %s, NOW())
    ON CONFLICT (fornecedor_original) DO UPDATE
    SET fornecedor_limpo = EXCLUDED.fornecedor_limpo,
        hash_fornecedor = EXCLUDED.hash_fornecedor,
        atualizado_em = NOW()
"""


def main():
    conn = get_db_conn()
    inserted = 0
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SELECT_SQL)
                rows = cur.fetchall()

                for (fornecedor_original,) in rows:
                    limpo = clean_supplier_name(fornecedor_original)
                    if not limpo:
                        continue
                    hsh = hash_supplier(limpo)
                    cur.execute(UPSERT_SQL, (fornecedor_original, limpo, hsh))
                    inserted += 1

        print(f"Fornecedores unicos processados: {inserted}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
