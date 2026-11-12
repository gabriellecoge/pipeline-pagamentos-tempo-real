-- Camada 3 (Ingestão): evidência de INSERT, UPDATE e DELETE virando evento.
-- Rode com:  bash scripts/demo_cdc.sh
-- Depois veja as 3 mensagens no tópico pg.public.transacoes (Kafka UI, porta 8080).

-- INSERT  -> evento com op = 'c' (create)
INSERT INTO transacoes (id_conta, id_cartao, valor, comerciante)
VALUES (1, 'CARD-DEMO', 42.90, 'Padaria Demo')
RETURNING id_transacao, id_cartao, status;

-- UPDATE  -> evento com op = 'u', com "before" (aprovada) e "after" (estornada)
UPDATE transacoes SET status = 'estornada'
 WHERE id_cartao = 'CARD-DEMO'
RETURNING id_transacao, id_cartao, status;

-- DELETE  -> evento com op = 'd', "before" preenchido e "after" vazio
DELETE FROM transacoes
 WHERE id_cartao = 'CARD-DEMO'
RETURNING id_transacao, id_cartao, status;
