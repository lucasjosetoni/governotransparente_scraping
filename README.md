python3 -m venv venv

source venv/bin/activate


GET /app/portal/api/v1/json/despesa/consolidada/empenho/03769490?ano=1&limit=-1&inicio=01/02/2023&fim=31/12/2023 HTTP/2
GET /app/portal/api/v1/json/despesa/consolidada/empenho/03769490?ano=2&limit=-1&inicio=01/02/2023&fim=31/12/2023 HTTP/2
GET /app/portal/api/v1/json/despesa/consolidada/empenho/03769490?ano=3&limit=-1&inicio=01/02/2023&fim=31/12/2023 HTTP/2



docker compose up
docker compose down -v
docker compose up -d

python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"



docker exec -it arquivos-airflow-scheduler-1 python /opt/airflow/dags/rasper_json_dag.py
docker exec -it arquivos-airflow-scheduler-1 python /opt/airflow/dags/popula_banco.py



docker exec -it $(docker ps -qf "name=airflow-worker|airflow-scheduler" | head -n 1) \
airflow connections add 'postgres_transparencia' \
    --conn-type 'postgres' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-host 'postgres' \
    --conn-port '5432' \
    --conn-schema 'transparencia'