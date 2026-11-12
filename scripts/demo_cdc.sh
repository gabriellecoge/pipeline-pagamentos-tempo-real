#!/usr/bin/env bash
# Camada 3 (Ingestão): faz INSERT, UPDATE e DELETE no Postgres e mostra os 3 eventos no Kafka.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "================ NO BANCO ================"
docker compose exec -T postgres psql -U postgres -d pagamentos < sql/postgres/demo_insert_update_delete.sql

echo "esperando o CDC entregar..."
sleep 8

echo
echo "================ NO KAFKA (tópico pg.public.transacoes) ================"
docker compose exec -T schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic pg.public.transacoes --from-beginning \
  --property schema.registry.url=http://localhost:8081 \
  --timeout-ms 15000 2>/dev/null \
  | grep "CARD-DEMO" \
  | python3 -c '
import json, sys
for linha in sys.stdin:
    e = json.loads(linha)
    tira = lambda v: list(v.values())[0] if isinstance(v, dict) and len(v) == 1 else v
    antes, depois = tira(e.get("before")), tira(e.get("after"))
    st_antes = antes["status"] if antes else None
    st_depois = depois["status"] if depois else None
    print("op=%s  status antes=%s  status depois=%s" % (e["op"], st_antes, st_depois))
    print("   evento:", json.dumps({k: e[k] for k in ("op", "before", "after")}, ensure_ascii=False))
'
