-- Camada 4 (Processamento): os dois jobs que ficam rodando sem parar.
-- Executado pelo scripts/subir.sh logo depois do 00_tabelas.sql.

SET 'execution.checkpointing.interval' = '10s';

-- JOB 1: enriquecimento
-- Junta cada transação com a conta (titular, cidade, segmento, limite)
-- e marca valor_alto quando o gasto passa de metade do limite.
SET 'pipeline.name' = 'enriquecimento';

INSERT INTO transacoes_enriquecidas
SELECT
  t.id_transacao,
  t.id_cartao,
  t.valor,
  t.comerciante,
  CAST(t.momento_evento AS STRING),
  c.nome_titular,
  c.cidade,
  c.segmento,
  c.limite,
  t.valor > c.limite * 0.5
FROM transacoes_eventos AS t
LEFT JOIN contas AS c
  ON t.id_conta = c.id_conta;

-- JOB 2: regra de fraude
-- 3 ou mais transações no MESMO cartão dentro de 60 segundos = alerta.
--
-- Por que janela OVER (deslizante) e não TUMBLE (fixa)?
-- A fixa corta o relógio em blocos de 1 minuto: compras às 00:50, 00:55 e
-- 01:05 cairiam em blocos diferentes e escapariam. A OVER olha os 60 s
-- ANTERIORES a cada transação, então qualquer trio dentro de 60 s é pego.
SET 'pipeline.name' = 'regra-fraude-3-em-60s';

INSERT INTO alertas_fraude
SELECT
  id_cartao,
  id_transacao,
  id_conta,
  valor,
  comerciante,
  qtd_60s,
  CAST(momento_evento AS STRING),
  'TRES_OU_MAIS_EM_60S'
FROM (
  SELECT
    id_cartao, id_transacao, id_conta, valor, comerciante, momento_evento,
    COUNT(*) OVER (
      PARTITION BY id_cartao
      ORDER BY momento_evento
      RANGE BETWEEN INTERVAL '60' SECOND PRECEDING AND CURRENT ROW
    ) AS qtd_60s
  FROM transacoes_eventos
)
WHERE qtd_60s >= 3;
