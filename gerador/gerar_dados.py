"""
Gerador de dados sintéticos de pagamentos.

- Garante as 20 contas (o banco já sobe com elas).
- Insere transações em ritmo contínuo no Postgres.
- De tempos em tempos dispara uma "rajada": 4 transações no MESMO cartão
  em poucos segundos — é isso que a regra de fraude deve pegar.

Uso:
    python gerador/gerar_dados.py              # roda até Ctrl+C
    python gerador/gerar_dados.py --minutos 5  # roda por 5 minutos
"""

import argparse
import os
import random
import time
from datetime import datetime

import psycopg
from dotenv import load_dotenv

load_dotenv()

CIDADES = ["Vitória", "Vila Velha", "Serra", "Cariacica", "São Paulo", "Rio de Janeiro"]
SEGMENTOS = ["varejo", "varejo", "varejo", "premium"]
COMERCIANTES = ["Mercado Sol", "Farmácia Vida", "Posto Rota", "Livraria Página",
                "Café Grão", "Loja Online X", "Restaurante Sabor", "Eletro Mais"]
NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Felipe", "Gabi", "Heitor",
         "Isis", "João", "Karen", "Leo", "Marina", "Nina", "Otávio", "Paula",
         "Rafa", "Sofia", "Tiago", "Vera"]


def conectar():
    return psycopg.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=os.environ.get("PG_PORT", "5432"),
        dbname=os.environ.get("PG_DB", "pagamentos"),
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ["PG_PASSWORD"],
        autocommit=True,
    )


def criar_contas(cur):
    for i, nome in enumerate(NOMES, start=1):
        cur.execute(
            """INSERT INTO contas (id_conta, nome_titular, cidade, segmento, limite)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (id_conta) DO NOTHING""",
            (i, nome, random.choice(CIDADES), random.choice(SEGMENTOS),
             random.choice([1500, 3000, 5000, 10000])),
        )
    print(f"[contas] {len(NOMES)} contas garantidas")


def inserir_transacao(cur, id_conta, id_cartao, valor=None):
    valor = valor or round(random.uniform(5, 800), 2)
    cur.execute(
        """INSERT INTO transacoes (id_conta, id_cartao, valor, comerciante)
           VALUES (%s, %s, %s, %s) RETURNING id_transacao""",
        (id_conta, id_cartao, valor, random.choice(COMERCIANTES)),
    )
    id_tx = cur.fetchone()[0]
    agora = datetime.now().strftime("%H:%M:%S")
    print(f"[{agora}] tx {id_tx:>6}  conta {id_conta:>2}  {id_cartao}  R$ {valor:>8.2f}")


def cartao_da_conta(id_conta):
    # cada conta tem 1 cartão fixo, para a regra "mesmo cartão" fazer sentido
    return f"CARD-{id_conta:04d}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutos", type=float, default=0, help="0 = sem limite")
    parser.add_argument("--rajada-a-cada", type=int, default=45,
                        help="segundos entre rajadas suspeitas")
    args = parser.parse_args()

    fim = time.time() + args.minutos * 60 if args.minutos else None
    proxima_rajada = time.time() + 15

    with conectar() as conn, conn.cursor() as cur:
        criar_contas(cur)
        try:
            while fim is None or time.time() < fim:
                if time.time() >= proxima_rajada:
                    id_conta = random.randint(1, len(NOMES))
                    print(f"--- RAJADA SUSPEITA no {cartao_da_conta(id_conta)} ---")
                    for _ in range(4):
                        inserir_transacao(cur, id_conta, cartao_da_conta(id_conta))
                        time.sleep(random.uniform(1, 4))
                    proxima_rajada = time.time() + args.rajada_a_cada
                else:
                    id_conta = random.randint(1, len(NOMES))
                    inserir_transacao(cur, id_conta, cartao_da_conta(id_conta))
                    # intervalo normal: o mesmo cartão raramente repete em 60s
                    time.sleep(random.uniform(1.5, 4))
        except KeyboardInterrupt:
            print("\nEncerrado pelo usuário.")


if __name__ == "__main__":
    main()
