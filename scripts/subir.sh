#!/usr/bin/env bash
# Sobe o pipeline inteiro: Postgres, Kafka, Schema Registry, Connect (CDC), Flink e Kafka UI.
# Pode rodar de novo sem problema: ele não duplica nada.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || { cp .env.example .env; echo "Criei o .env a partir do .env.example."; }

echo "== 1/4 Subindo os containers =="
echo "   (na primeira vez baixa as imagens: uns 5 a 10 minutos)"
docker compose up -d --build

echo "== 2/4 Esperando o Kafka Connect ficar pronto =="
until curl -sf http://localhost:8083/connectors > /dev/null; do sleep 5; printf '.'; done; echo " ok"

echo "== 3/4 Registrando o conector de CDC (Postgres -> Kafka) =="
curl -sf -X PUT -H "Content-Type: application/json" \
  --data @config/conector-postgres-cdc.json \
  http://localhost:8083/connectors/cdc-postgres-pagamentos/config > /dev/null

estado_tarefa() {
  curl -s http://localhost:8083/connectors/cdc-postgres-pagamentos/status | python3 -c '
import json, sys
try:
    s = json.load(sys.stdin); t = s.get("tasks", [])
    print(t[0]["state"] if t else "AGUARDANDO")
except Exception:
    print("AGUARDANDO")'
}
for i in $(seq 1 60); do
  E=$(estado_tarefa)
  [ "$E" = "RUNNING" ] && break
  if [ "$E" = "FAILED" ]; then
    echo; echo "O conector falhou. Detalhes:"
    curl -s http://localhost:8083/connectors/cdc-postgres-pagamentos/status; echo
    exit 1
  fi
  sleep 3; printf '.'
done; echo " conector: $E"

echo "== 4/4 Iniciando os jobs do Flink SQL =="
until curl -sf http://localhost:8082/overview > /dev/null; do sleep 3; printf '.'; done
RODANDO=$(curl -s http://localhost:8082/jobs/overview | python3 -c '
import json, sys
print(sum(1 for j in json.load(sys.stdin)["jobs"] if j["state"] == "RUNNING"))')
if [ "$RODANDO" -ge 2 ]; then
  echo "   os jobs já estão rodando ($RODANDO), nada a fazer"
else
  docker compose run --rm sql-client bin/sql-client.sh -i /sql/00_tabelas.sql -f /sql/01_jobs.sql
fi

cat <<'FIM'

Pipeline no ar!
  Kafka UI (tópicos, mensagens, schemas, conector, ACLs): porta 8080
  Painel do Flink (jobs rodando):                          porta 8082
  (no Codespaces: aba PORTAS, embaixo, e clique no globo da porta)

Próximos passos (veja o README):
  python gerador/gerar_dados.py        # gera transações
  python consumidor/ler_alertas.py     # lê os alertas (em outro terminal)
FIM
