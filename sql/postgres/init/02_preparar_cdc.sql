-- Camada 3 (Ingestão): o que o conector de CDC precisa do banco.
-- Roda sozinho na primeira vez que o container do Postgres sobe.
--
-- Pré-requisito 1: wal_level = logical
--   Já ligado no docker-compose.yml (command: -c wal_level=logical).
--   Confira depois com:  SHOW wal_level;
--
-- Pré-requisito 2: usuário com permissão de replicação
--   O usuário "postgres" é superusuário, então já tem.

-- Pré-requisito 3: REPLICA IDENTITY FULL
--   Faz o UPDATE e o DELETE carregarem a linha inteira de "antes" no evento.
--   Sem isso, o "before" vem só com a chave primária.
ALTER TABLE contas     REPLICA IDENTITY FULL;
ALTER TABLE transacoes REPLICA IDENTITY FULL;

-- Pré-requisito 4: publication
--   A lista de tabelas que o Postgres "anuncia" para quem lê o log (o conector).
CREATE PUBLICATION pub_pagamentos FOR TABLE contas, transacoes;

-- O replication slot (o "marcador de leitura" do conector) é criado pelo
-- próprio conector quando ele sobe.
