#!/usr/bin/env bash
# Camada 1 (Fundação): tópicos + permissões das duas identidades.
# Roda sozinho dentro do container kafka-init, pela porta interna (admin).
set -euo pipefail
B="--bootstrap-server kafka:29092"

echo "== Tópicos (1 partição cada: pouco volume, ordem garantida) =="
for T in pg.public.contas pg.public.transacoes transacoes_enriquecidas alertas_fraude; do
  kafka-topics $B --create --if-not-exists --topic "$T" --partitions 1 --replication-factor 1
done

echo "== Identidade de ESCRITA: escritor (usada pelo conector de CDC) =="
# só mexe nos tópicos que começam com "pg." e nos tópicos internos do Connect
kafka-acls $B --add --allow-principal User:escritor --operation All \
  --topic pg. --resource-pattern-type prefixed
kafka-acls $B --add --allow-principal User:escritor --operation All \
  --topic _connect- --resource-pattern-type prefixed
kafka-acls $B --add --allow-principal User:escritor --operation All \
  --group connect- --resource-pattern-type prefixed
kafka-acls $B --add --allow-principal User:escritor \
  --operation Describe --operation DescribeConfigs --operation IdempotentWrite --cluster

echo "== Identidade de LEITURA: leitor (usada pelo consumidor de alertas) =="
# só LÊ os tópicos de saída; não escreve nada e não vê os dados brutos do banco
for T in alertas_fraude transacoes_enriquecidas; do
  kafka-acls $B --add --allow-principal User:leitor \
    --operation Read --operation Describe --topic "$T"
done
kafka-acls $B --add --allow-principal User:leitor --operation Read \
  --group leitor- --resource-pattern-type prefixed

echo "== Permissões criadas =="
kafka-acls $B --list
