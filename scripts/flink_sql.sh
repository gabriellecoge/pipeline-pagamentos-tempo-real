#!/usr/bin/env bash
# Abre o terminal interativo do Flink SQL, já com as tabelas declaradas.
# Lá dentro, digite por exemplo:
#   SELECT * FROM transacoes_enriquecidas;
#   SELECT * FROM alertas_fraude;
# Ctrl+C para parar a consulta, QUIT; para sair.
cd "$(dirname "$0")/.."
docker compose run --rm sql-client bin/sql-client.sh -i /sql/00_tabelas.sql
