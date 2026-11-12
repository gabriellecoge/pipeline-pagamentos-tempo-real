# Apache Flink 1.19 + conector Kafka + formato Avro com Schema Registry.

FROM flink:1.19.1-scala_2.12-java11

ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.19.1/flink-sql-avro-confluent-registry-1.19.1.jar /opt/flink/lib/
RUN chmod 644 /opt/flink/lib/*.jar
