-- =============================
-- EXTENSÕES
-- =============================
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =============================
-- SCHEMAS
-- =============================
CREATE SCHEMA IF NOT EXISTS raw;  -- staging / entrada
CREATE SCHEMA IF NOT EXISTS dw;   -- dados tratados

-- =============================
-- TABELA STAGING (RAW)
-- =============================
CREATE TABLE IF NOT EXISTS raw.empenhos_staging (
    id_empenho VARCHAR(50),
    num_empenho VARCHAR(50),

    orgao TEXT,
    fornecedor TEXT,

    data_iso DATE,
    historico TEXT,

    empenhado NUMERIC(15,2),
    liquidado NUMERIC(15,2),
    gasto NUMERIC(15,2),

    row_hash TEXT
);

-- Índice leve só pra ajudar merge
CREATE INDEX IF NOT EXISTS idx_staging_id
    ON raw.empenhos_staging (id_empenho);

-- =============================
-- TABELA FINAL (DW)
-- =============================
CREATE TABLE IF NOT EXISTS dw.empenhos (
    id_empenho VARCHAR(50) PRIMARY KEY,
    num_empenho VARCHAR(50),

    orgao TEXT,
    fornecedor TEXT,

    data_iso DATE,
    historico TEXT,

    empenhado NUMERIC(15,2) DEFAULT 0,
    liquidado NUMERIC(15,2) DEFAULT 0,
    gasto NUMERIC(15,2) DEFAULT 0,

    row_hash TEXT NOT NULL,

    data_versao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================
-- ÍNDICES (performance real)
-- =============================

CREATE INDEX IF NOT EXISTS idx_dw_empenhos_data
    ON dw.empenhos (data_iso);

CREATE INDEX IF NOT EXISTS idx_dw_empenhos_orgao
    ON dw.empenhos (orgao);

CREATE INDEX IF NOT EXISTS idx_dw_empenhos_fornecedor
    ON dw.empenhos (fornecedor);

-- Para comparação de mudança (UPSERT)
CREATE INDEX IF NOT EXISTS idx_dw_empenhos_hash
    ON dw.empenhos (row_hash);

-- =============================
-- BOAS PRÁTICAS (opcional)
-- =============================

-- Evita lixo na staging (você pode truncar sempre antes de usar)
-- TRUNCATE raw.empenhos_staging;

-- =============================
-- PERMISSÕES
-- =============================
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO airflow;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dw TO airflow;