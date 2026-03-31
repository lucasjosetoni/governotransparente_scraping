import json
import os

from common import get_db_conn, normalize_for_compare, only_digits

try:
    import google.generativeai as genai
except ImportError:
    genai = None


SELECT_PENDING_SQL = """
    SELECT fornecedor_original, hash_fornecedor
    FROM raw.fornecedores_unicos
    WHERE hash_fornecedor NOT IN (
        SELECT hash_fornecedor
        FROM raw.fornecedores_normalizados
    )
    ORDER BY fornecedor_original
"""

UPSERT_SQL = """
    INSERT INTO raw.fornecedores_normalizados (
        hash_fornecedor,
        fornecedor_original,
        fornecedor_normalizado,
        grupo_deduplicacao,
        cnpj_sugerido_llm,
        confianca_llm,
        modelo_llm,
        status_normalizacao,
        observacao,
        processado_em,
        atualizado_em
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
    ON CONFLICT (hash_fornecedor) DO UPDATE
    SET fornecedor_original = EXCLUDED.fornecedor_original,
        fornecedor_normalizado = EXCLUDED.fornecedor_normalizado,
        grupo_deduplicacao = EXCLUDED.grupo_deduplicacao,
        cnpj_sugerido_llm = EXCLUDED.cnpj_sugerido_llm,
        confianca_llm = EXCLUDED.confianca_llm,
        modelo_llm = EXCLUDED.modelo_llm,
        status_normalizacao = EXCLUDED.status_normalizacao,
        observacao = EXCLUDED.observacao,
        processado_em = NOW(),
        atualizado_em = NOW()
"""


def normalize_rule_based(nome):
    canonical = normalize_for_compare(nome)
    return {
        "fornecedor_normalizado": canonical,
        "grupo_deduplicacao": canonical,
        "cnpj_sugerido_llm": only_digits(nome)[:14] if len(only_digits(nome)) >= 14 else None,
        "confianca_llm": 0.50,
        "status_normalizacao": "fallback_rule_based",
        "observacao": "Gemini indisponivel; normalizacao por regra local.",
        "modelo_llm": None,
    }


def ask_gemini(model, nome):
    prompt = (
        "Normalize nome de fornecedor para um nome canonico sem abreviacoes desnecessarias. "
        "Retorne JSON valido com as chaves: fornecedor_normalizado, grupo_deduplicacao, "
        "cnpj_sugerido_llm, confianca_llm, observacao. "
        "Se nao souber CNPJ, use null em cnpj_sugerido_llm. "
        f"Fornecedor: {nome}"
    )
    response = model.generate_content(prompt)
    text = (response.text or "").strip()

    if text.startswith("```"):
        text = text.strip("`")
        text = text.replace("json", "", 1).strip()

    payload = json.loads(text)

    cnpj = payload.get("cnpj_sugerido_llm")
    if cnpj:
        cnpj = only_digits(str(cnpj))[:14] or None

    return {
        "fornecedor_normalizado": payload.get("fornecedor_normalizado") or normalize_for_compare(nome),
        "grupo_deduplicacao": payload.get("grupo_deduplicacao") or normalize_for_compare(nome),
        "cnpj_sugerido_llm": cnpj,
        "confianca_llm": float(payload.get("confianca_llm") or 0.0),
        "status_normalizacao": "normalizado_llm",
        "observacao": payload.get("observacao"),
        "modelo_llm": os.getenv("GEMINI_MODEL", "gemini-1.5-flash"),
    }


def main():
    conn = get_db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SELECT_PENDING_SQL)
                rows = cur.fetchall()

                api_key = os.getenv("GEMINI_API_KEY")
                use_gemini = bool(api_key and genai is not None)
                model = None

                if use_gemini:
                    genai.configure(api_key=api_key)
                    model = genai.GenerativeModel(os.getenv("GEMINI_MODEL", "gemini-1.5-flash"))

                for fornecedor_original, hash_fornecedor in rows:
                    if use_gemini:
                        try:
                            normalized = ask_gemini(model, fornecedor_original)
                        except Exception as exc:
                            normalized = normalize_rule_based(fornecedor_original)
                            normalized["observacao"] = f"Fallback por erro Gemini: {exc}"
                    else:
                        normalized = normalize_rule_based(fornecedor_original)

                    cur.execute(
                        UPSERT_SQL,
                        (
                            hash_fornecedor,
                            fornecedor_original,
                            normalized["fornecedor_normalizado"],
                            normalized["grupo_deduplicacao"],
                            normalized["cnpj_sugerido_llm"],
                            normalized["confianca_llm"],
                            normalized["modelo_llm"],
                            normalized["status_normalizacao"],
                            normalized["observacao"],
                        ),
                    )

        print(f"Fornecedores normalizados: {len(rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
