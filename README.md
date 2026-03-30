python3 -m venv venv

source venv/bin/activate


GET /app/portal/api/v1/json/despesa/consolidada/empenho/03769490?ano=1&limit=-1&inicio=01/02/2023&fim=31/12/2023 HTTP/2
GET /app/portal/api/v1/json/despesa/consolidada/empenho/03769490?ano=2&limit=-1&inicio=01/02/2023&fim=31/12/2023 HTTP/2
GET /app/portal/api/v1/json/despesa/consolidada/empenho/03769490?ano=3&limit=-1&inicio=01/02/2023&fim=31/12/2023 HTTP/2



docker compose up