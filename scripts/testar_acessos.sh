#!/usr/bin/env bash
# Camada 1 (Fundação): prova que as duas identidades têm só o acesso que precisam.
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

docker compose exec -T -e SENHA="$LEITOR_SENHA" kafka bash -s <<'EOF'
cat > /tmp/leitor.properties <<P
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="leitor" password="$SENHA";
P

echo "================ PERMISSÕES CADASTRADAS ================"
kafka-acls --bootstrap-server kafka:29092 --list 2>/dev/null

echo
echo "================ TESTE 1: leitor tenta ESCREVER em alertas_fraude (deve ser NEGADO) ================"
echo '{"falso":"alerta"}' | timeout 30 kafka-console-producer \
  --bootstrap-server kafka:29095 --producer.config /tmp/leitor.properties \
  --topic alertas_fraude 2>&1 | grep -oE "[A-Za-z]*AuthorizationException[^,]*" | head -1

echo
echo "================ TESTE 2: leitor tenta LER os dados brutos do banco (deve ser NEGADO) ================"
timeout 30 kafka-console-consumer \
  --bootstrap-server kafka:29095 --consumer.config /tmp/leitor.properties \
  --topic pg.public.transacoes --group leitor-teste --from-beginning \
  --max-messages 1 --timeout-ms 15000 2>&1 | grep -oE "[A-Za-z]*AuthorizationException[^,]*" | head -1

echo
echo "================ TESTE 3: leitor LÊ alertas_fraude (deve FUNCIONAR) ================"
timeout 30 kafka-console-consumer \
  --bootstrap-server kafka:29095 --consumer.config /tmp/leitor.properties \
  --topic alertas_fraude --group leitor-teste --from-beginning \
  --max-messages 3 --timeout-ms 15000 > /dev/null 2> /tmp/saida.txt
grep -E "Processed a total|Exception" /tmp/saida.txt | head -1
EOF
