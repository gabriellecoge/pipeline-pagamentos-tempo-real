#!/usr/bin/env bash
# Derruba tudo: containers, dados, tópicos e o replication slot (que some junto com o banco).
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose --profile ferramentas down -v --remove-orphans

cat <<'FIM'

Tudo derrubado. Nada fica rodando e nenhum dado sobra.

Se estiver no GitHub Codespaces, pare também a máquina para não gastar
suas horas grátis: menu (☰) no canto superior esquerdo > Codespaces >
Stop Current Codespace. Ou em github.com/codespaces > ... > Stop.
FIM
