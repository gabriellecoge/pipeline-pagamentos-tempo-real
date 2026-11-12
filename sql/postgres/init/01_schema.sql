-- Camada 3 (Ingestão) — tabelas de origem no Postgres
-- Rodar no SQL Editor do Neon (ou via psql).

CREATE TABLE IF NOT EXISTS contas (
    id_conta      INTEGER PRIMARY KEY,
    nome_titular  TEXT        NOT NULL,
    cidade        TEXT        NOT NULL,
    segmento      TEXT        NOT NULL,          -- ex.: 'varejo', 'premium'
    limite        NUMERIC(12,2) NOT NULL,
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transacoes (
    id_transacao  BIGSERIAL PRIMARY KEY,
    id_conta      INTEGER     NOT NULL REFERENCES contas(id_conta),
    id_cartao     TEXT        NOT NULL,          -- ex.: 'CARD-0003' (dado sintético)
    valor         NUMERIC(12,2) NOT NULL,
    comerciante   TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'aprovada',
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now()
);
