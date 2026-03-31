from common import get_db_conn


UPSERT_ALIAS_SQL = """
    INSERT INTO dw.fornecedor_alias (
        fornecedor_original,
        hash_fornecedor,
        fornecedor_normalizado,
        id_fornecedor,
        atualizado_em
    )
    SELECT
        n.fornecedor_original,
        n.hash_fornecedor,
        n.fornecedor_normalizado,
        e.id_fornecedor,
        NOW()
    FROM raw.fornecedores_normalizados n
    LEFT JOIN dw.fornecedores_enriquecidos e
      ON e.fornecedor_normalizado = n.fornecedor_normalizado
    ON CONFLICT (fornecedor_original) DO UPDATE
    SET hash_fornecedor = EXCLUDED.hash_fornecedor,
        fornecedor_normalizado = EXCLUDED.fornecedor_normalizado,
        id_fornecedor = EXCLUDED.id_fornecedor,
        atualizado_em = NOW()
"""


def main():
    conn = get_db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(UPSERT_ALIAS_SQL)
                upserts = cur.rowcount

        print(f"Aliases atualizados: {upserts}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
