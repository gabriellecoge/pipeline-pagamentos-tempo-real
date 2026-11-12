# Kafka Connect da Confluent (já traz o conversor Avro) + conector Debezium para Postgres.
# O Debezium é o mesmo motor de CDC usado pelo conector gerenciado do Confluent Cloud.

FROM alpine:3.20 AS baixar
ARG DEBEZIUM_VERSION=2.5.4.Final
ADD https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz /tmp/debezium.tar.gz
RUN mkdir -p /plugins && tar -xzf /tmp/debezium.tar.gz -C /plugins

FROM confluentinc/cp-kafka-connect:7.7.1
COPY --from=baixar /plugins/ /usr/share/confluent-hub-components/
