"""
Consumidor dos alertas de fraude, usando a identidade de LEITURA ("leitor").

Lê o tópico alertas_fraude, que o Flink escreve em Avro com o schema
registrado no Schema Registry, e imprime cada alerta.

Uso:
    python consumidor/ler_alertas.py
    python consumidor/ler_alertas.py --topico transacoes_enriquecidas
"""

import argparse
import os

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

load_dotenv()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topico", default="alertas_fraude")
    args = parser.parse_args()

    sr = SchemaRegistryClient({"url": os.environ.get("SR_URL", "http://localhost:8081")})
    desserializar = AvroDeserializer(sr)

    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"),
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "leitor",
        "sasl.password": os.environ["LEITOR_SENHA"],
        "group.id": "leitor-alertas",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([args.topico])
    print(f"Lendo '{args.topico}' com a identidade 'leitor'... (Ctrl+C para sair)\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Erro:", msg.error())
                continue
            if msg.value() is None:  # registro apagado (tombstone)
                continue
            valor = desserializar(msg.value(),
                                  SerializationContext(msg.topic(), MessageField.VALUE))
            if args.topico == "alertas_fraude":
                print(f"🚨 ALERTA  cartão {valor['id_cartao']}  "
                      f"{valor['qtd_60s']} transações em 60 s  "
                      f"(tx {valor['id_transacao']}, R$ {valor['valor']:.2f}, "
                      f"{valor['comerciante']}, {valor['momento_alerta']})")
            else:
                print(valor)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
