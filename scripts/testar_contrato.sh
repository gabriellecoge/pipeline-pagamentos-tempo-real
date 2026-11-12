#!/usr/bin/env bash
# Camada 2 (Contrato): registra o schema e testa se ele aguenta mudanças.
#   v1 -> registra
#   v2 -> adiciona campo opcional com default             => deve ser COMPATÍVEL
#   v3 -> muda o tipo de "valor" + campo obrigatório novo => deve ser RECUSADA
set -uo pipefail
cd "$(dirname "$0")/.."

SR="http://localhost:8081"
SUBJECT="contrato-transacao-value"
CT="Content-Type: application/vnd.schemaregistry.v1+json"
corpo() { python3 -c 'import json,sys; print(json.dumps({"schema": open(sys.argv[1]).read()}))' "$1"; }

echo "-> regra de compatibilidade do subject: BACKWARD"
curl -s -H "$CT" -X PUT "$SR/config/$SUBJECT" -d '{"compatibility":"BACKWARD"}'; echo

echo; echo "-> registrando v1"
curl -s -H "$CT" -X POST "$SR/subjects/$SUBJECT/versions" -d "$(corpo schemas/transacao-v1.avsc)"; echo

echo; echo "-> v2 (novo campo opcional 'canal') é compatível?"
curl -s -H "$CT" -X POST "$SR/compatibility/subjects/$SUBJECT/versions/latest?verbose=true" \
  -d "$(corpo schemas/transacao-v2-compativel.avsc)"; echo
echo "-> registrando v2"
curl -s -H "$CT" -X POST "$SR/subjects/$SUBJECT/versions" -d "$(corpo schemas/transacao-v2-compativel.avsc)"; echo

echo; echo "-> v3 ('valor' vira texto + campo obrigatório 'moeda') é compatível?"
curl -s -H "$CT" -X POST "$SR/compatibility/subjects/$SUBJECT/versions/latest?verbose=true" \
  -d "$(corpo schemas/transacao-v3-incompativel.avsc)"; echo
echo "-> tentando registrar v3 mesmo assim (esperado: erro 409)"
curl -s -H "$CT" -X POST "$SR/subjects/$SUBJECT/versions" -d "$(corpo schemas/transacao-v3-incompativel.avsc)"; echo

echo; echo "-> versões aceitas no subject:"
curl -s "$SR/subjects/$SUBJECT/versions"; echo

echo; echo "-> schemas que o conector registrou sozinho a partir do banco:"
curl -s "$SR/subjects"; echo
