import difflib
import time

import requests

from common import get_db_conn, normalize_for_compare, only_digits


SELECT_SQL = """
    SELECT hash_fornecedor, fornecedor_normalizado, cnpj_sugerido_llm, confianca_llm
    FROM raw.fornecedores_normalizados
    WHERE status_normalizacao IN ('normalizado_llm', 'fallback_rule_based')
"""

UPSERT_SQL = """
    INSERT INTO dw.fornecedores_enriquecidos (
        fornecedor_normalizado,
        cnpj_sugerido_llm,
        cnpj_validado,
        razao_social,
        nome_fantasia,
        uf,
        municipio,
        situacao_cadastral,
        fonte_validacao,
        status_enriquecimento,
        confianca_llm,
        atualizado_em
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (fornecedor_normalizado) DO UPDATE
    SET cnpj_sugerido_llm = EXCLUDED.cnpj_sugerido_llm,
        cnpj_validado = EXCLUDED.cnpj_validado,
        razao_social = EXCLUDED.razao_social,
        nome_fantasia = EXCLUDED.nome_fantasia,
        uf = EXCLUDED.uf,
        municipio = EXCLUDED.municipio,
        situacao_cadastral = EXCLUDED.situacao_cadastral,
        fonte_validacao = EXCLUDED.fonte_validacao,
        status_enriquecimento = EXCLUDED.status_enriquecimento,
        confianca_llm = EXCLUDED.confianca_llm,
        atualizado_em = NOW()
"""


def score_name_match(nome_normalizado, razao_social, nome_fantasia):
    base = normalize_for_compare(nome_normalizado)
    options = [normalize_for_compare(razao_social), normalize_for_compare(nome_fantasia)]
    scores = [difflib.SequenceMatcher(None, base, opt).ratio() for opt in options if opt]
    return max(scores) if scores else 0.0


def query_brasilapi(cnpj):
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
    response = requests.get(url, timeout=30)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def main():
    conn = get_db_conn()
    checked = 0
    success = 0
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SELECT_SQL)
                rows = cur.fetchall()

                for _, fornecedor_normalizado, cnpj_sugerido_llm, confianca_llm in rows:
                    checked += 1
                    cnpj = only_digits(cnpj_sugerido_llm)
                    if len(cnpj) != 14:
                        cur.execute(
                            UPSERT_SQL,
                            (
                                fornecedor_normalizado,
                                cnpj_sugerido_llm,
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                "brasilapi",
                                "sem_cnpj_sugerido",
                                confianca_llm,
                            ),
                        )
                        continue

                    try:
                        payload = query_brasilapi(cnpj)
                        if payload is None:
                            cur.execute(
                                UPSERT_SQL,
                                (
                                    fornecedor_normalizado,
                                    cnpj_sugerido_llm,
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                    "brasilapi",
                                    "nao_encontrado",
                                    confianca_llm,
                                ),
                            )
                            continue

                        razao_social = payload.get("razao_social")
                        nome_fantasia = payload.get("nome_fantasia")
                        match_score = score_name_match(
                            fornecedor_normalizado,
                            razao_social,
                            nome_fantasia,
                        )

                        status = "validado" if match_score >= 0.55 else "divergencia_nome"
                        cnpj_validado = cnpj if status == "validado" else None
                        if status == "validado":
                            success += 1

                        cur.execute(
                            UPSERT_SQL,
                            (
                                fornecedor_normalizado,
                                cnpj_sugerido_llm,
                                cnpj_validado,
                                razao_social,
                                nome_fantasia,
                                payload.get("uf"),
                                payload.get("municipio"),
                                payload.get("descricao_situacao_cadastral"),
                                "brasilapi",
                                status,
                                confianca_llm,
                            ),
                        )

                    except Exception as exc:
                        cur.execute(
                            UPSERT_SQL,
                            (
                                fornecedor_normalizado,
                                cnpj_sugerido_llm,
                                None,
                                None,
                                None,
                                None,
                                None,
                                str(exc)[:200],
                                "brasilapi",
                                "erro_api",
                                confianca_llm,
                            ),
                        )

                    time.sleep(0.35)

        print(f"Consultados: {checked} | Validados: {success}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
