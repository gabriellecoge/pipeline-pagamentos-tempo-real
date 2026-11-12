-- Camada 4 (Processamento): declaração das tabelas do Flink.
-- Cada tabela aponta para um tópico do Kafka; nenhum dado é copiado aqui.

SET 'table.local-time-zone' = 'America/Sao_Paulo';
SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'tableau';

-- 1) Transações vindas do CDC, lidas como uma SEQUÊNCIA DE EVENTOS.
--    O conector grava o "envelope" do Debezium: before, after, op, ts_ms.
--    Ler o envelope "cru" faz cada transação virar um fato com hora marcada,
--    que é o que a regra de janela de tempo precisa.
CREATE TABLE transacoes_cdc (
  `before` ROW<id_transacao BIGINT, id_conta INT, id_cartao STRING, valor DOUBLE,
              comerciante STRING, status STRING, criado_em STRING>,
  `after`  ROW<id_transacao BIGINT, id_conta INT, id_cartao STRING, valor DOUBLE,
              comerciante STRING, status STRING, criado_em STRING>,
  `op`     STRING,
  `ts_ms`  BIGINT,
  -- hora do evento = momento em que o CDC capturou a mudança
  momento_evento AS TO_TIMESTAMP_LTZ(`ts_ms`, 3),
  -- watermark: "um evento pode chegar até 5 s atrasado"
  WATERMARK FOR momento_evento AS momento_evento - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg.public.transacoes',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-transacoes',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://schema-registry:8081'
);

-- Só as transações novas: op 'c' (criada) e 'r' (carga inicial).
CREATE VIEW transacoes_eventos AS
SELECT
  `after`.id_transacao AS id_transacao,
  `after`.id_conta     AS id_conta,
  `after`.id_cartao    AS id_cartao,
  `after`.valor        AS valor,
  `after`.comerciante  AS comerciante,
  momento_evento
FROM transacoes_cdc
WHERE `op` IN ('c', 'r');

-- 2) Contas, lidas como TABELA QUE SE ATUALIZA (changelog).
--    O formato debezium-avro-confluent entende insert/update/delete sozinho,
--    então esta tabela sempre mostra o estado atual de cada conta.
CREATE TABLE contas (
  id_conta     INT,
  nome_titular STRING,
  cidade       STRING,
  segmento     STRING,
  limite       DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg.public.contas',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-contas',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-avro-confluent',
  'value.debezium-avro-confluent.url' = 'http://schema-registry:8081'
);

-- 3) Saída: transação + dados da conta (upsert por id_transacao).
CREATE TABLE transacoes_enriquecidas (
  id_transacao BIGINT,
  id_cartao    STRING,
  valor        DOUBLE,
  comerciante  STRING,
  momento      STRING,
  nome_titular STRING,
  cidade       STRING,
  segmento     STRING,
  limite       DOUBLE,
  valor_alto   BOOLEAN,
  PRIMARY KEY (id_transacao) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'transacoes_enriquecidas',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'avro-confluent',
  'key.avro-confluent.url' = 'http://schema-registry:8081',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://schema-registry:8081'
);

-- 4) Saída: alertas de fraude (só inserções).
CREATE TABLE alertas_fraude (
  id_cartao      STRING,
  id_transacao   BIGINT,
  id_conta       INT,
  valor          DOUBLE,
  comerciante    STRING,
  qtd_60s        BIGINT,
  momento_alerta STRING,
  regra          STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'alertas_fraude',
  'properties.bootstrap.servers' = 'kafka:29092',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://schema-registry:8081'
);
