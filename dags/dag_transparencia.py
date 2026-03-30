from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json
import hashlib
import os
import pandas as pd

# Configurações básicas
DATA_PATH = "/opt/airflow/data/raw_data.json"
CONN_ID = "postgres_default"

def extrair_dados(**kwargs):
    url = "https://governotransparente.com.br/app/portal/api/v1/json/despesa/consolidada/empenho/03769490"
    params = {"ano": "3", "limit": "-1", "inicio": "01/01/2023", "fim": "31/12/2023"}
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    
    with open(DATA_PATH, 'w', encoding='utf-8') as f:
        json.dump(response.json(), f)
    return True

def verificar_mudanca_checksum():
    # Aqui você compararia o MD5 do arquivo atual com o último salvo no banco
    # Se forem iguais, retorna False para interromper a DAG (Short-Circuit)
    return True 

def carga_postgres():
    pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
    with open(DATA_PATH, 'r') as f:
        dados = json.load(f)
    
    # Query SQL com o MD5 sendo gerado pelo Postgres no momento da inserção
    upsert_query = """
    INSERT INTO empenhos_macapa (
        id_empenho, num_empenho, orgao, fornecedor, data_iso, historico, empenhado, liquidado, gasto, row_hash
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        md5(concat(%s, %s, %s, %s)) -- Hash baseado nos campos mutáveis
    )
    ON CONFLICT (id_empenho) DO UPDATE SET
        liquidado = EXCLUDED.liquidado,
        gasto = EXCLUDED.gasto,
        historico = EXCLUDED.historico,
        row_hash = EXCLUDED.row_hash,
        data_versao = NOW()
    WHERE empenhos_macapa.row_hash IS DISTINCT FROM EXCLUDED.row_hash;
    """
    
    for item in dados:
        # Preparação mínima para o banco (conversão de data)
        data_fmt = datetime.strptime(item['dataDesc'], '%d/%m/%Y').date()
        
        params = (
            item['idEmpenho'], item['empenho'], item['orgao'], item['fornecedor'], data_fmt, item['historico'],
            item['empenhado'], item['liquidado'], item['gasto'],
            # Repetimos campos para o md5(concat(...))
            item['liquidado'], item['gasto'], item['historico'], item['orgao']
        )
        pg_hook.run(upsert_query, parameters=params)

with DAG(
    'etl_transparencia_macapa',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='extrair_json', python_callable=extrair_dados)
    
    t2 = ShortCircuitOperator(task_id='check_mudanca', python_callable=verificar_mudanca_checksum)
    
    t3 = PythonOperator(task_id='carga_postgres', python_callable=carga_postgres)

    t1 >> t2 >> t3