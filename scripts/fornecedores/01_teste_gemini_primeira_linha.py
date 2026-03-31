import json
import os
import sys
import logging
import time
import re
from typing import Dict, Any

from google import genai
from google.genai import types
from dotenv import load_dotenv

# Importações do seu projeto Conecta
from common import get_db_conn, normalize_for_compare, sanitize_for_llm

# Configuração de Logging para produção
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

SELECT_TEN_SQL = """
    SELECT fornecedor_original, hash_fornecedor
    FROM raw.fornecedores_normalizados
    WHERE cnpj_sugerido_llm IS NULL
    ORDER BY atualizado_em NULLS FIRST, hash_fornecedor
    LIMIT %s
"""

SELECT_UNICOS_FALLBACK_SQL = """
    SELECT u.fornecedor_original, u.hash_fornecedor
    FROM raw.fornecedores_unicos u
    LEFT JOIN raw.fornecedores_normalizados n
      ON n.hash_fornecedor = u.hash_fornecedor
    WHERE n.hash_fornecedor IS NULL
    ORDER BY u.atualizado_em NULLS FIRST, u.hash_fornecedor
    LIMIT %s
"""

INSERT_NORMALIZADOS_BASE_SQL = """
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
    VALUES (%s, %s, %s, %s, NULL, NULL, NULL, 'pendente', 'Seed de fallback a partir de fornecedores_unicos', NULL, NOW())
    ON CONFLICT (hash_fornecedor) DO NOTHING
"""

UPDATE_SQL = """
    UPDATE raw.fornecedores_normalizados
    SET cnpj_sugerido_llm = %s,
        confianca_llm = %s,
        modelo_llm = %s,
        status_normalizacao = %s,
        observacao = %s,
        processado_em = NOW(),
        atualizado_em = NOW()
    WHERE hash_fornecedor = %s
"""

def setup_client():
    """Configura o cliente oficial da nova SDK google-genai."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY não encontrada nas variáveis de ambiente.")
        sys.exit(1)
    
    return genai.Client(api_key=api_key)

def clean_json_string(text: str) -> str:
    """Remove blocos de código markdown (```json ... ```) da resposta."""
    # Remove blocos markdown e espaços extras
    text = re.sub(r'```json\s*|```\s*', '', text).strip()
    return text


def normalize_cnpj(cnpj: str):
    if not cnpj:
        return None
    digits = re.sub(r"\D", "", str(cnpj))
    if len(digits) != 14:
        return None
    return digits if is_valid_cnpj(digits) else None


def is_valid_cnpj(cnpj: str) -> bool:
    if not cnpj or len(cnpj) != 14 or len(set(cnpj)) == 1:
        return False

    def calc_digit(base: str, weights):
        total = sum(int(num) * weight for num, weight in zip(base, weights))
        remainder = total % 11
        return "0" if remainder < 2 else str(11 - remainder)

    d1 = calc_digit(cnpj[:12], [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    d2 = calc_digit(cnpj[:12] + d1, [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    return cnpj[-2:] == d1 + d2


def is_quota_error(message: str) -> bool:
    msg = (message or "").lower()
    return "resource_exhausted" in msg or "quota exceeded" in msg or "429" in msg


def extract_retry_seconds(message: str) -> float | None:
    msg = message or ""
    match = re.search(r"retry in\s+([0-9]+(?:\.[0-9]+)?)s", msg, flags=re.IGNORECASE)
    if match:
        return float(match.group(1))
    match = re.search(r"retryDelay[^0-9]*([0-9]+)s", msg, flags=re.IGNORECASE)
    if match:
        return float(match.group(1))
    return None

def process_fornecedor(client: genai.Client, nome_original: str) -> Dict[str, Any]:
    """Pesquisa na internet somente o CNPJ matriz do fornecedor."""
    model_id = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
    
    # Configuração da Ferramenta de Busca (Google Search)
    search_tool = types.Tool(google_search=types.GoogleSearch())

    # Prompt instruindo o formato, já que o MIME type fixo causa erro com Tools
    nome_limpo = sanitize_for_llm(nome_original)
    nome_canonico = normalize_for_compare(nome_limpo)

    prompt = (
        f"Pesquise na internet APENAS o CNPJ MATRIZ do fornecedor brasileiro: {nome_canonico}. "
        "Responda EXCLUSIVAMENTE com JSON valido contendo somente as chaves: "
        "cnpj_sugerido_llm (string ou null), confianca_llm (number entre 0 e 1), observacao (string). "
        "Nao inclua texto fora do JSON."
    )

    try:
        response = client.models.generate_content(
            model=model_id,
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[search_tool],
                temperature=0.0,
                system_instruction="Você é um assistente técnico de higienização de dados. Responda apenas com JSON puro."
            )
        )
        
        # Limpeza da resposta para evitar erro de parse
        json_payload = clean_json_string(response.text)
        result = json.loads(json_payload)
        result["cnpj_sugerido_llm"] = normalize_cnpj(result.get("cnpj_sugerido_llm"))

        return {
            "cnpj_sugerido_llm": result.get("cnpj_sugerido_llm"),
            "confianca_llm": float(result.get("confianca_llm") or 0.0),
            "observacao": result.get("observacao") or "",
            "error_type": None,
            "retry_seconds": None,
        }
    
    except Exception as e:
        err = str(e)
        quota_error = is_quota_error(err)
        retry_seconds = extract_retry_seconds(err)
        logger.error(f"Erro ao processar '{nome_original}': {err}")
        return {
            "cnpj_sugerido_llm": None,
            "confianca_llm": 0.0,
            "observacao": f"Erro tecnico na extracao: {err}",
            "error_type": "quota_exceeded" if quota_error else "llm_error",
            "retry_seconds": retry_seconds,
        }

def main():
    # Inicialização
    client = setup_client()
    conn = get_db_conn()
    sleep_seconds = float(os.getenv("GEMINI_SLEEP_SECONDS", "1.0"))
    batch_size = int(os.getenv("GEMINI_BATCH_SIZE", "10"))
    dry_run = os.getenv("GEMINI_DRY_RUN", "false").strip().lower() in ("1", "true", "yes", "y")
    stop_on_quota = os.getenv("GEMINI_STOP_ON_QUOTA", "true").strip().lower() in ("1", "true", "yes", "y")
    
    try:
        with conn.cursor() as cur:
            cur.execute(SELECT_TEN_SQL, (batch_size,))
            rows = cur.fetchall()

        if not rows:
            logger.info("Sem pendentes em fornecedores_normalizados. Tentando fallback por fornecedores_unicos.")
            with conn.cursor() as cur:
                cur.execute(SELECT_UNICOS_FALLBACK_SQL, (batch_size,))
                fallback_rows = cur.fetchall()

            for fornecedor_original, hash_id in fallback_rows:
                fornecedor_limpo = sanitize_for_llm(fornecedor_original)
                fornecedor_canonico = normalize_for_compare(fornecedor_limpo)
                with conn.cursor() as cur:
                    cur.execute(
                        INSERT_NORMALIZADOS_BASE_SQL,
                        (hash_id, fornecedor_original, fornecedor_canonico, fornecedor_canonico),
                    )
                conn.commit()

            with conn.cursor() as cur:
                cur.execute(SELECT_TEN_SQL, (batch_size,))
                rows = cur.fetchall()

        if not rows:
            logger.info("Nenhum fornecedor elegivel para processamento.")
            return

        logger.info(f"Iniciando busca ativa para {len(rows)} fornecedores")
        logger.info(f"Modo dry-run: {dry_run}")

        for idx, (fornecedor_original, hash_id) in enumerate(rows, start=1):
            fornecedor_limpo = sanitize_for_llm(fornecedor_original)
            fornecedor_canonico = normalize_for_compare(fornecedor_limpo)
            logger.info(f"[{idx}/{len(rows)}] Processando: {fornecedor_original}")
            logger.info(f"[{idx}/{len(rows)}] Sanitizado: {fornecedor_limpo}")
            logger.info(f"[{idx}/{len(rows)}] Canonico: {fornecedor_canonico}")

            resultado = process_fornecedor(client, fornecedor_original)
            if resultado.get("error_type") == "quota_exceeded":
                status_normalizacao = "erro_quota_llm"
            elif resultado.get("cnpj_sugerido_llm"):
                status_normalizacao = "normalizado_llm"
            else:
                status_normalizacao = "sem_cnpj_llm"

            if not dry_run:
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            UPDATE_SQL,
                            (
                                resultado.get("cnpj_sugerido_llm"),
                                resultado.get("confianca_llm"),
                                os.getenv("GEMINI_MODEL", "gemini-1.5-flash"),
                                status_normalizacao,
                                resultado.get("observacao"),
                                hash_id,
                            ),
                        )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            time.sleep(sleep_seconds)
            print("\n" + "=" * 60)
            print(f"ITEM: {idx}/{len(rows)}")
            print(f"FORNECEDOR: {fornecedor_original}")
            print(f"FORNECEDOR_SANITIZADO: {fornecedor_limpo}")
            print(f"FORNECEDOR_CANONICO: {fornecedor_canonico}")
            print(f"HASH: {hash_id}")
            print("-" * 60)
            print(json.dumps(resultado, indent=2, ensure_ascii=False))
            print(f"STATUS_GRAVACAO: {status_normalizacao}")
            print(f"DRY_RUN: {dry_run}")
            print("=" * 60 + "\n")

            if resultado.get("error_type") == "quota_exceeded" and stop_on_quota:
                wait_hint = resultado.get("retry_seconds")
                if wait_hint:
                    logger.warning(
                        f"Quota do Gemini excedida. Sugestao de espera: {wait_hint:.1f}s. Encerrando lote para evitar falso negativo em massa."
                    )
                else:
                    logger.warning("Quota do Gemini excedida. Encerrando lote para evitar falso negativo em massa.")
                break

    except Exception as exc:
        logger.error(f"Falha na execução principal: {exc}")
    finally:
        conn.close()
        logger.info("Conexão com o banco finalizada.")

if __name__ == "__main__":
    main()