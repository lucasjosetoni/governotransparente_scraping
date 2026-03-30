from airflow import DAG
from airflow.ops.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import time
import os

# Configurações padrão
default_args = {
    'owner': 'Lucas Toni',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
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
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    try:
        print(f"📥 Extraindo: Ano ID {ano_id} [{inicio} a {fim}]")
        response = requests.get(base_url, params=params, headers=headers, timeout=60)
        response.raise_for_status()
        
        dados = response.json()
        
        if not dados:
            print(f"⚠️ Aviso: Nenhum dado retornado.")
            return

        # Nome do arquivo usando o início do período
        sufixo = inicio.replace('/', '-')
        filename = f"raw_empenhos_{sufixo}.json"
        filepath = os.path.join(data_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=4)
            
        print(f"✅ Arquivo salvo em: {filepath} ({len(dados)} registros)")
        
    except Exception as e:
        print(f"❌ Falha na extração: {e}")
        raise  # Força a falha da Task no Airflow para retry

# Definição da DAG
with DAG(
    'extracao_transparencia_macapa',
    default_args=default_args,
    description='Extrai dados brutos da API de transparência',
    schedule_interval=None, # Rodar manualmente ou definir ex: '@daily'
    catchup=False,
    tags=['scraping', 'macapa'],
) as dag:

    # Lista de períodos conforme seu script original
    periodos = [
        {"ano_id": 3, "inicio": "01/01/2023", "fim": "31/12/2023"},
        {"ano_id": 1, "inicio": "01/01/2024", "fim": "31/12/2024"},
        {"ano_id": 2, "inicio": "01/01/2025", "fim": "31/12/2025"}
    ]

    # Criando as tasks dinamicamente
    for p in periodos:
        task_id = f"extrair_{p['inicio'].replace('/', '_')}"
        
        PythonOperator(
            task_id=task_id,
            python_callable=extrair_json_bruto,
            op_kwargs=p
        )