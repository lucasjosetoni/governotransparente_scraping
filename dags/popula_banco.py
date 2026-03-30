import os
import hashlib
import pandas as pd
from datetime import datetime
from sqlalchemy import text

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- CONFIG ---
DATA_DIR = "/opt/airflow/data"
POSTGRES_CONN_ID = "postgres_transparencia"

# --- AUX ---
def calcular_checksum(filepath):
    if not os.path.exists(filepath):
        return None

    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


def listar_arquivos_pendentes(pg_hook):
    return pg_hook.get_records("""
        SELECT nome_arquivo, checksum_md5
        FROM raw.controle_arquivos
        WHERE processado = FALSE
        ORDER BY data_extracao ASC
    """)


def extrair_periodo_referencia(nome_arquivo):
    base = nome_arquivo.replace("raw_empenhos_", "").replace(".json", "")

    if "_ref_" in base:
        base = base.split("_ref_", 1)[0]

    partes = base.split("_")
    if len(partes) >= 2:
        return f"{partes[-2]}_{partes[-1]}"

    return None


def obter_checksum_ultimo_processado(pg_hook, periodo_referencia, nome_arquivo_atual):
    return pg_hook.get_first(
        """
        SELECT checksum_md5
        FROM raw.controle_arquivos
        WHERE periodo_referencia = %(periodo_referencia)s
          AND processado = TRUE
          AND nome_arquivo <> %(nome_arquivo_atual)s
        ORDER BY COALESCE(data_processamento, data_extracao) DESC
        LIMIT 1
        """,
        parameters={
            "periodo_referencia": periodo_referencia,
            "nome_arquivo_atual": nome_arquivo_atual,
        },
    )

# --- TASKS ---

def check_for_changes(**kwargs):
    print("🔎 Verificando mudanças...")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    pendentes = listar_arquivos_pendentes(pg_hook)

    if not pendentes:
        print("✨ Sem arquivos pendentes. Abortando DAG.")
        return False

    print(f"⚠️ {len(pendentes)} arquivo(s) pendente(s) para processar.")
    kwargs['ti'].xcom_push(key='arquivos_pendentes', value=[p[0] for p in pendentes])
    return True

def processar_json_para_postgres(**kwargs):
    print("🚀 Iniciando ETL (modo produção)...")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    arquivos_pendentes = kwargs['ti'].xcom_pull(
        key='arquivos_pendentes',
        task_ids='check_file_changes'
    )

    arquivos = [os.path.join(DATA_DIR, nome) for nome in (arquivos_pendentes or [])]
    if not arquivos:
        print("⚠️ Nenhum arquivo encontrado.")
        return

    for arquivo in arquivos:
        print(f"📂 Processando: {arquivo}")

        nome_arquivo = os.path.basename(arquivo)
        periodo_referencia = extrair_periodo_referencia(nome_arquivo)

        if not periodo_referencia:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE raw.controle_arquivos
                    SET status = 'erro',
                        mensagem_erro = :mensagem
                    WHERE nome_arquivo = :nome_arquivo
                """), {
                    "nome_arquivo": nome_arquivo,
                    "mensagem": "Nao foi possivel extrair periodo_referencia do nome do arquivo.",
                })
            print(f"❌ Nome de arquivo fora do padrão: {nome_arquivo}")
            continue

        if not os.path.exists(arquivo):
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE raw.controle_arquivos
                    SET status = 'erro',
                        mensagem_erro = :mensagem
                    WHERE nome_arquivo = :nome_arquivo
                """), {
                    "nome_arquivo": nome_arquivo,
                    "mensagem": f"Arquivo não encontrado em {arquivo}",
                })
            print(f"❌ Arquivo não encontrado: {arquivo}")
            continue

        checksum_arquivo = calcular_checksum(arquivo)
        ultimo_processado = obter_checksum_ultimo_processado(
            pg_hook,
            periodo_referencia,
            nome_arquivo,
        )

        if ultimo_processado and checksum_arquivo == ultimo_processado[0]:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE raw.controle_arquivos
                    SET processado = TRUE,
                        status = 'sucesso',
                        linhas_carregadas = 0,
                        data_processamento = NOW(),
                        mensagem_erro = NULL
                    WHERE nome_arquivo = :nome_arquivo
                """), {"nome_arquivo": nome_arquivo})

            print(
                f"✅ Sem mudanças para o período {periodo_referencia}. "
                "Carga ignorada."
            )
            continue

        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE raw.controle_arquivos
                SET status = 'processando',
                    mensagem_erro = NULL
                WHERE nome_arquivo = :nome_arquivo
            """), {"nome_arquivo": nome_arquivo})

        try:
            df = pd.read_json(arquivo)

            if df.empty:
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE raw.controle_arquivos
                        SET processado = TRUE,
                            status = 'sucesso',
                            linhas_carregadas = 0,
                            data_processamento = NOW(),
                            mensagem_erro = NULL
                        WHERE nome_arquivo = :nome_arquivo
                    """), {"nome_arquivo": nome_arquivo})
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

                conn.execute(text("""
                    UPDATE raw.controle_arquivos
                    SET processado = TRUE,
                        status = 'sucesso',
                        linhas_carregadas = :linhas_carregadas,
                        data_processamento = NOW(),
                        mensagem_erro = NULL
                    WHERE nome_arquivo = :nome_arquivo
                """), {
                    "nome_arquivo": nome_arquivo,
                    "linhas_carregadas": result.rowcount if result.rowcount is not None else 0,
                })

                print(f"✅ Linhas afetadas: {result.rowcount}")

        except Exception as e:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE raw.controle_arquivos
                    SET status = 'erro',
                        mensagem_erro = :mensagem_erro
                    WHERE nome_arquivo = :nome_arquivo
                """), {
                    "nome_arquivo": nome_arquivo,
                    "mensagem_erro": str(e),
                })
            raise


# --- DAG ---
with DAG(
    dag_id='etl_empenhos_v1',
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

    check_changes >> process_data