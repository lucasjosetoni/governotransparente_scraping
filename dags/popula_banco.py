import json
import glob
import os
import pandas as pd
from sqlalchemy import create_engine, text
from airflow.models import Variable

# No Docker, usamos o nome do serviço 'postgres' em vez de 'localhost'
DB_URL = "postgresql://airflow:airflow@postgres:5432/transparencia"
JSON_FILES_PATH = "/opt/airflow/data/*.json"

def processar_json_para_postgres():
    # O engine deve ser criado dentro da função para evitar problemas de serialização no Airflow
    engine = create_engine(DB_URL)
    arquivos = glob.glob(JSON_FILES_PATH)
    
    if not arquivos:
        print("⚠️ Nenhum arquivo JSON encontrado em /opt/airflow/data/")
        return

    # Query com a lógica de MD5 e Upsert
    query = text("""
        INSERT INTO empenhos_macapa (
            id_empenho, num_empenho, orgao, fornecedor, data_iso, 
            historico, empenhado, liquidado, gasto, row_hash
        ) VALUES (
            :id_empenho, :num_empenho, :orgao, :fornecedor, :data_iso, 
            :historico, :empenhado, :liquidado, :gasto,
            md5(concat(
                COALESCE(:liquidado::text, ''), 
                COALESCE(:gasto::text, ''), 
                COALESCE(:historico, ''), 
                COALESCE(:orgao, '')
            ))
        )
        ON CONFLICT (id_empenho) DO UPDATE SET
            liquidado = EXCLUDED.liquidado,
            gasto = EXCLUDED.gasto,
            historico = EXCLUDED.historico,
            row_hash = EXCLUDED.row_hash,
            data_versao = NOW()
        WHERE empenhos_macapa.row_hash IS DISTINCT FROM EXCLUDED.row_hash;
    """)

    for arquivo in arquivos:
        print(f"📦 Lendo JSON: {os.path.basename(arquivo)}")
        
        with open(arquivo, 'r', encoding='utf-8') as f:
            dados = json.load(f)
        
        if not dados:
            continue
            
        df = pd.DataFrame(dados)

        # 1. Tratamento de Data
        df['data_iso'] = pd.to_datetime(df['dataDesc'], format='%d/%m/%Y', errors='coerce').dt.date
        
        # 2. Limpeza de Histórico
        df['historico'] = df['historico'].fillna('').replace(r'\s+', ' ', regex=True).str.strip()
        
        # 3. Mapeamento e preenchimento de NAs para evitar erro no SQL
        df = df.rename(columns={
            'idEmpenho': 'id_empenho',
            'empenho': 'num_empenho'
        })
        
        # Garante que colunas numéricas tenham valores válidos
        for col in ['empenhado', 'liquidado', 'gasto']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 4. Execução do Upsert em bloco (mais performático)
        with engine.begin() as conn:
            registros = df.to_dict(orient='records')
            for reg in registros:
                # Filtrando apenas as chaves que a query espera
                params = {k: reg.get(k) for k in [
                    'id_empenho', 'num_empenho', 'orgao', 'fornecedor', 
                    'data_iso', 'historico', 'empenhado', 'liquidado', 'gasto'
                ]}
                conn.execute(query, params)
        
        print(f"✅ Arquivo {os.path.basename(arquivo)} processado. Total: {len(df)} registros.")

if __name__ == "__main__":
    processar_json_para_postgres()