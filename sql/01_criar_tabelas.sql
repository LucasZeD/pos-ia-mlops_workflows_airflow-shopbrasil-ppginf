-- =============================================================================
-- Tabelas da base analítica (shopbrasil_dw)
-- Os containers postgres-dw executam este script automaticamente nas primeiras
-- inicializações. As DAGs também executam os mesmos DDLs (CREATE IF NOT EXISTS)
-- como medidas de segurança.
-- =============================================================================

-- 1) SNAPSHOT (idempotente)
-- Armazenam-se linhas únicas por (data_referencia, categoria). As PRIMARY KEYs
-- garantem que, nos reprocessamentos de uma mesma execução, as linhas sejam
-- ATUALIZADAS (upsert) em vez de duplicadas. Estas tabelas alimentam os painéis
-- das equipes de pricing.
CREATE TABLE IF NOT EXISTS metricas_categoria_snapshot (
    data_referencia DATE          NOT NULL,
    categoria       VARCHAR(100)  NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_min       NUMERIC(12,2) NOT NULL,
    preco_max       NUMERIC(12,2) NOT NULL,
    atualizado_em   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (data_referencia, categoria)
);

-- 2) HISTÓRICO (append)
-- As execuções acrescentam novas linhas, contendo os run_ids e os horários das
-- coletas. Estas tabelas permitem acompanhar as evoluções dos preços ao longo do tempo.
CREATE TABLE IF NOT EXISTS metricas_categoria_historico (
    id              BIGSERIAL     PRIMARY KEY,
    data_referencia DATE          NOT NULL,
    run_id          VARCHAR(250)  NOT NULL,
    categoria       VARCHAR(100)  NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_min       NUMERIC(12,2) NOT NULL,
    preco_max       NUMERIC(12,2) NOT NULL,
    coletado_em     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Criam-se índices por categoria e data, otimizando-se as consultas históricas.
CREATE INDEX IF NOT EXISTS idx_historico_categoria_data
    ON metricas_categoria_historico (categoria, data_referencia);
