from airflow import DAG
from airflow.operators.python import PythonOperator # CORRIGIDO AQUI
from datetime import datetime, timedelta
import requests
import json
import os
import hashlib

from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_transparencia"


def calcular_md5(filepath):
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def registrar_arquivo_no_banco(nome_arquivo, checksum_md5, tamanho_bytes, periodo_referencia):
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = """
        INSERT INTO raw.controle_arquivos (
            nome_arquivo,
            checksum_md5,
            tamanho_bytes,
            data_extracao,
            processado,
            status,
            periodo_referencia
        )
        VALUES (
            %(nome_arquivo)s,
            %(checksum_md5)s,
            %(tamanho_bytes)s,
            NOW(),
            FALSE,
            'pendente',
            %(periodo_referencia)s
        )
        ON CONFLICT (nome_arquivo) DO UPDATE SET
            checksum_md5 = EXCLUDED.checksum_md5,
            tamanho_bytes = EXCLUDED.tamanho_bytes,
            data_extracao = NOW(),
            periodo_referencia = EXCLUDED.periodo_referencia,
            status = CASE
                WHEN raw.controle_arquivos.checksum_md5 = EXCLUDED.checksum_md5
                    THEN raw.controle_arquivos.status
                ELSE 'pendente'
            END,
            processado = CASE
                WHEN raw.controle_arquivos.checksum_md5 = EXCLUDED.checksum_md5
                    THEN raw.controle_arquivos.processado
                ELSE FALSE
            END,
            mensagem_erro = CASE
                WHEN raw.controle_arquivos.checksum_md5 = EXCLUDED.checksum_md5
                    THEN raw.controle_arquivos.mensagem_erro
                ELSE NULL
            END;
    """

    pg_hook.run(
        sql,
        parameters={
            "nome_arquivo": nome_arquivo,
            "checksum_md5": checksum_md5,
            "tamanho_bytes": tamanho_bytes,
            "periodo_referencia": periodo_referencia,
        },
    )

# Configurações padrão
default_args = {
    'owner': 'Lucas Toni',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1), # Data no passado para permitir execução manual
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extrair_json_bruto(ano_id, inicio, fim, limit="-1"):
    """
    Função adaptada para rodar dentro do Worker do Airflow.
    """
    base_url = "https://governotransparente.com.br/app/portal/api/v1/json/despesa/consolidada/empenho/03769490"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json",
        "Referer": "https://governotransparente.com.br/"
    }

    params = {
        "ano": str(ano_id),
        "limit": str(limit),
        "inicio": inicio,
        "fim": fim
    }

    # No Docker, usamos o caminho absoluto mapeado no volume
    data_dir = "/opt/airflow/data"
    os.makedirs(data_dir, exist_ok=True)

    try:
        print(f"📥 Extraindo: Ano ID {ano_id} [{inicio} a {fim}]")
        response = requests.get(base_url, params=params, headers=headers, timeout=60)
        response.raise_for_status()
        
        dados = response.json()
        
        if not dados:
            print(f"⚠️ Aviso: Nenhum dado retornado.")
            return

        inicio_fmt = inicio.replace('/', '-')
        fim_fmt = fim.replace('/', '-')
        filename = f"raw_empenhos_{inicio_fmt}_{fim_fmt}.json"
        filepath = os.path.join(data_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=4)

        checksum_md5 = calcular_md5(filepath)
        tamanho_bytes = os.path.getsize(filepath)
        registrar_arquivo_no_banco(
            nome_arquivo=filename,
            checksum_md5=checksum_md5,
            tamanho_bytes=tamanho_bytes,
            periodo_referencia=inicio_fmt,
        )
            
        print(
            f"✅ Arquivo salvo em: {filepath} ({len(dados)} registros) | "
            f"MD5: {checksum_md5}"
        )
        
    except Exception as e:
        print(f"❌ Falha na extração: {e}")
        raise 

# Definição da DAG
with DAG(
    'extracao_transparencia_v1',
    default_args=default_args,
    description='Extrai dados brutos da API de transparência',
    schedule=None, # Mudado de schedule_interval para schedule
    catchup=False,
    tags=['scraping', 'portal_transparencia'],
) as dag:

    periodos = [
        {"ano_id": 3, "inicio": "01/01/2023", "fim": "31/12/2023"},
        {"ano_id": 1, "inicio": "01/01/2024", "fim": "31/12/2024"},
        {"ano_id": 2, "inicio": "01/01/2025", "fim": "31/12/2025"}
    ]

    for p in periodos:
        task_id = f"extrair_{p['inicio'].replace('/', '_')}"
        
        PythonOperator(
            task_id=task_id,
            python_callable=extrair_json_bruto,
            op_kwargs=p
        )