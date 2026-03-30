import json
import glob
import os
import hashlib
import pandas as pd
from datetime import datetime
from sqlalchemy import text

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- CONFIG ---
JSON_FILES_PATH = "/opt/airflow/data/raw_empenhos_01-01-2023_30-03-2023.json"
CHECKSUM_VAR_KEY = "last_checksum_empenhos_macapa"
POSTGRES_CONN_ID = "postgres_transparencia"

# --- AUX ---
def calcular_checksum():
    if not os.path.exists(JSON_FILES_PATH):
        print("⚠️ Arquivo não encontrado.")
        return None

    hash_md5 = hashlib.md5()
    with open(JSON_FILES_PATH, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()

# --- TASKS ---

def check_for_changes(**kwargs):
    print("🔎 Verificando mudanças...")

    current_checksum = calcular_checksum()
    if not current_checksum:
        return False

    last_checksum = Variable.get(CHECKSUM_VAR_KEY, default_var=None)

    if current_checksum == last_checksum:
        print("✨ Sem mudanças. Abortando DAG.")
        return False

    print("⚠️ Mudança detectada!")
    kwargs['ti'].xcom_push(key='new_checksum', value=current_checksum)
    return True


def processar_json_para_postgres():
    print("🚀 Iniciando ETL (modo produção)...")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    arquivos = glob.glob(JSON_FILES_PATH)
    if not arquivos:
        print("⚠️ Nenhum arquivo encontrado.")
        return

    for arquivo in arquivos:
        print(f"📂 Processando: {arquivo}")

        df = pd.read_json(arquivo)

        if df.empty:
            print("⚠️ DataFrame vazio.")
            continue

        # -------------------------
        # TRANSFORMAÇÃO
        # -------------------------
        df['data_iso'] = pd.to_datetime(
            df['dataDesc'],
            format='%d/%m/%Y',
            errors='coerce'
        ).dt.date

        df['historico'] = (
            df['historico']
            .fillna('')
            .replace(r'\s+', ' ', regex=True)
            .str.strip()
        )

        df = df.rename(columns={
            'idEmpenho': 'id_empenho',
            'empenho': 'num_empenho'
        })

        for col in ['empenhado', 'liquidado', 'gasto']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # -------------------------
        # HASH (vetorizado)
        # -------------------------
        df['row_hash'] = (
            df['liquidado'].astype(str) +
            df['gasto'].astype(str) +
            df['historico'] +
            df['orgao']
        ).apply(lambda x: hashlib.md5(x.encode()).hexdigest())

        # Apenas colunas necessárias
        df = df[[
            'id_empenho', 'num_empenho', 'orgao', 'fornecedor',
            'data_iso', 'historico',
            'empenhado', 'liquidado', 'gasto',
            'row_hash'
        ]]

        # -------------------------
        # CARGA (STAGING + MERGE)
        # -------------------------
        with engine.begin() as conn:

            print("🧹 Limpando staging...")
            conn.execute(text("TRUNCATE raw.empenhos_staging"))

            print("📥 Inserindo staging...")
            df.to_sql(
                'empenhos_staging',
                conn,
                schema='raw',
                if_exists='append',
                index=False,
                method='multi'
            )

            print("🔄 Fazendo merge (upsert)...")

            merge_sql = text("""
                INSERT INTO dw.empenhos (
                    id_empenho, num_empenho, orgao, fornecedor, data_iso,
                    historico, empenhado, liquidado, gasto, row_hash
                )
                SELECT 
                    id_empenho,
                    num_empenho,
                    orgao,
                    fornecedor,
                    data_iso,
                    historico,
                    empenhado,
                    liquidado,
                    gasto,
                    row_hash
                FROM raw.empenhos_staging
                ON CONFLICT (id_empenho) DO UPDATE SET
                    liquidado = EXCLUDED.liquidado,
                    gasto = EXCLUDED.gasto,
                    historico = EXCLUDED.historico,
                    row_hash = EXCLUDED.row_hash,
                    data_versao = NOW()
                WHERE dw.empenhos.row_hash IS DISTINCT FROM EXCLUDED.row_hash;
            """)

            result = conn.execute(merge_sql)

            print(f"✅ Linhas afetadas: {result.rowcount}")


def update_checksum_variable(**kwargs):
    new_checksum = kwargs['ti'].xcom_pull(
        key='new_checksum',
        task_ids='check_file_changes'
    )

    if new_checksum:
        print(f"💾 Salvando checksum: {new_checksum}")
        Variable.set(CHECKSUM_VAR_KEY, new_checksum)


# --- DAG ---
with DAG(
    dag_id='etl_empenhos_v4',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['etl', 'postgres', 'transparencia']
) as dag:

    check_changes = ShortCircuitOperator(
        task_id='check_file_changes',
        python_callable=check_for_changes
    )

    process_data = PythonOperator(
        task_id='processar_json_para_postgres',
        python_callable=processar_json_para_postgres
    )

    update_state = PythonOperator(
        task_id='update_checksum_state',
        python_callable=update_checksum_variable
    )

    check_changes >> process_data >> update_state