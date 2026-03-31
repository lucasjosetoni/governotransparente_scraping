import difflib
import time
import json

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


def log_result(fornecedor_normalizado, cnpj_sugerido_llm, status, detalhes=""):
    print(
        f"[CNPJ] fornecedor='{fornecedor_normalizado}' | "
        f"cnpj_sugerido='{cnpj_sugerido_llm}' | status='{status}'"
        f"{f' | {detalhes}' if detalhes else ''}"
    )


def log_found_data(cnpj, payload):
    preview = {
        "cnpj": cnpj,
        "razao_social": payload.get("razao_social"),
        "nome_fantasia": payload.get("nome_fantasia"),
        "uf": payload.get("uf"),
        "municipio": payload.get("municipio"),
        "situacao_cadastral": payload.get("descricao_situacao_cadastral"),
    }
    print(f"[CNPJ_ENCONTRADO] {json.dumps(preview, ensure_ascii=False)}")


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
                        log_result(
                            fornecedor_normalizado,
                            cnpj_sugerido_llm,
                            "sem_cnpj_sugerido",
                            "cnpj ausente ou invalido",
                        )
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
                            log_result(
                                fornecedor_normalizado,
                                cnpj_sugerido_llm,
                                "nao_encontrado",
                                "brasilapi retornou 404",
                            )
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

                        log_found_data(cnpj, payload)

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

                        log_result(
                            fornecedor_normalizado,
                            cnpj_sugerido_llm,
                            status,
                            (
                                f"score={match_score:.3f} | "
                                f"razao_social='{razao_social}' | "
                                f"nome_fantasia='{nome_fantasia}'"
                            ),
                        )

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
                        log_result(
                            fornecedor_normalizado,
                            cnpj_sugerido_llm,
                            "erro_api",
                            f"erro='{str(exc)[:200]}'",
                        )
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
