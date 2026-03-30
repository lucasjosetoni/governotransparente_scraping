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

CREATE TABLE IF NOT EXISTS raw.controle_arquivos (
    nome_arquivo TEXT PRIMARY KEY,
    checksum_md5 TEXT,
    tamanho_bytes BIGINT,
    data_extracao TIMESTAMP DEFAULT NOW(),
    data_processamento TIMESTAMP,
    processado BOOLEAN DEFAULT FALSE,
    status VARCHAR(20) DEFAULT 'pendente',
    mensagem_erro TEXT,
    linhas_carregadas INTEGER,
    periodo_referencia TEXT
);

ALTER TABLE raw.controle_arquivos
    ADD COLUMN IF NOT EXISTS tamanho_bytes BIGINT;

ALTER TABLE raw.controle_arquivos
    ADD COLUMN IF NOT EXISTS data_processamento TIMESTAMP;

ALTER TABLE raw.controle_arquivos
    ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'pendente';

ALTER TABLE raw.controle_arquivos
    ADD COLUMN IF NOT EXISTS mensagem_erro TEXT;

ALTER TABLE raw.controle_arquivos
    ADD COLUMN IF NOT EXISTS linhas_carregadas INTEGER;

ALTER TABLE raw.controle_arquivos
    ADD COLUMN IF NOT EXISTS periodo_referencia TEXT;

ALTER TABLE raw.controle_arquivos
    ALTER COLUMN periodo_referencia TYPE TEXT;

CREATE INDEX IF NOT EXISTS idx_controle_processado_extracao
    ON raw.controle_arquivos (processado, data_extracao DESC);

CREATE INDEX IF NOT EXISTS idx_controle_status
    ON raw.controle_arquivos (status);

CREATE INDEX IF NOT EXISTS idx_controle_periodo
    ON raw.controle_arquivos (periodo_referencia);

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