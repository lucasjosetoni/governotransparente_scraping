# Projeto ETL de Empenhos - Airflow + PostgreSQL

Este projeto extrai dados de empenhos de um portal de transparencia, registra metadados de arquivos brutos para controle de processamento e carrega os dados tratados em um esquema analitico no PostgreSQL.

## Visao Geral

Fluxo principal:

1. Extracao via API em JSON por periodos pre-definidos.
2. Armazenamento do arquivo bruto em data com hash MD5 e metadados em raw.controle_arquivos.
3. Carga para staging (raw.empenhos_staging).
4. Merge para tabela final (dw.empenhos) com UPSERT por id_empenho e controle de mudanca por row_hash.

## Arquitetura

- Airflow: orquestracao das DAGs de extracao e carga.
- PostgreSQL: armazenamento de staging, controle e camada final.
- Docker Compose: subida local de toda a stack.

Componentes relevantes:

- dags/rasper_json_dag.py: extracao e registro do arquivo no controle.
- dags/popula_banco.py: validacao de arquivos pendentes, transformacao e carga.
- sql/init_db.sql: criacao de schemas, tabelas e indices.
- data: arquivos JSON brutos.
- logs: logs de execucao do Airflow.

## DAGs e Responsabilidades

### extracao_transparencia_v1

Arquivo: dags/rasper_json_dag.py

- Extrai dados por 3 periodos:
    - 01/01/2023 a 31/12/2023
    - 01/01/2024 a 31/12/2024
    - 01/01/2025 a 31/12/2025
- Salva arquivos no padrao:
    - raw_empenhos_{inicio}_{fim}_ref_{timestamp}.json
- Calcula MD5 e registra em raw.controle_arquivos.

### etl_empenhos_v1

Arquivo: dags/popula_banco.py

- Busca arquivos pendentes (processado = FALSE).
- Compara checksum com o ultimo arquivo processado do mesmo periodo.
- Se nao houver mudanca: marca como sucesso sem recarga.
- Se houver mudanca:
    - Carrega para raw.empenhos_staging.
    - Executa merge em dw.empenhos com ON CONFLICT.
    - Atualiza status, linhas carregadas e data de processamento.

## Pre-requisitos

- Docker e Docker Compose instalados.
- Acesso de rede ao endpoint da API de transparencia.
- Porta 5432 livre (PostgreSQL) e porta 8080 livre (Airflow webserver).

## Configuracao de Ambiente

Crie um arquivo .env na raiz do projeto com:

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=transparencia
AIRFLOW_UID=50000
AIRFLOW_FERNET_KEY=<gerar_chave>

Gerar AIRFLOW_FERNET_KEY:

python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

## Subindo o Ambiente

Na raiz do projeto:

docker compose up -d

Para desligar:

docker compose down

Para desligar removendo volumes:

docker compose down -v

## Inicializacao de Banco

O projeto contem o script sql/init_db.sql para criar schemas e tabelas.

Se necessario aplicar manualmente:

docker exec -i arquivos-postgres-1 psql -U airflow -d transparencia < sql/init_db.sql

## Configurando Conexao no Airflow

Crie a conexao postgres_transparencia dentro de um container Airflow:

docker exec -it $(docker ps -qf "name=airflow-worker|airflow-scheduler" | head -n 1) \
airflow connections add 'postgres_transparencia' \
    --conn-type 'postgres' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-host 'postgres' \
    --conn-port '5432' \
    --conn-schema 'transparencia'

## Executando o Fluxo

### Opcao 1: pela UI do Airflow

1. Acesse http://localhost:8080
2. Usuario padrao: admin
3. Senha padrao: admin
4. Execute na ordem:
     - extracao_transparencia_v1
     - etl_empenhos_v1

### Opcao 2: teste manual dentro do container

docker exec -it arquivos-airflow-scheduler-1 python /opt/airflow/dags/rasper_json_dag.py
docker exec -it arquivos-airflow-scheduler-1 python /opt/airflow/dags/popula_banco.py

## Validacao Rapida

Verificar se os containers estao saudaveis:

docker ps

Verificar registros de controle:

docker exec -it arquivos-postgres-1 psql -U airflow -d transparencia -c "SELECT nome_arquivo, processado, status, linhas_carregadas, periodo_referencia FROM raw.controle_arquivos ORDER BY data_extracao DESC LIMIT 20;"

Verificar carga final:

docker exec -it arquivos-postgres-1 psql -U airflow -d transparencia -c "SELECT count(*) FROM dw.empenhos;"

## Troubleshooting

### Nenhum arquivo pendente para processar

Comportamento esperado quando o checksum do periodo nao mudou. O ETL marca como sucesso e ignora recarga.

### Erro de conexao postgres_transparencia

Garanta que a conexao foi criada no Airflow com o nome exato postgres_transparencia.

### Arquivo nao encontrado no processamento

Confirme que o volume data esta montado e que os arquivos existem em data.

### Erro de inicializacao SQL

O docker-compose referencia ./sql/init.sql. Se o nome do arquivo no projeto for sql/init_db.sql, ajuste o compose ou aplique manualmente o script mostrado acima.

## Boas Praticas Operacionais

- Evite versionar logs e arquivos temporarios no Git.
- Mantenha uma unica DAG oficial por fluxo para reduzir ambiguidade entre versoes.
- Nao use credenciais padrao em producao.
- Monitore crescimento de dw.empenhos e planeje particionamento se necessario.

## Resumo Tecnico

- Extracao: API JSON -> data/raw_empenhos_*.json
- Controle: raw.controle_arquivos (checksum, status, processado)
- Staging: raw.empenhos_staging
- DW final: dw.empenhos com UPSERT e row_hash