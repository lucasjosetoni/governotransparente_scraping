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

-- =============================
-- FORNECEDORES (ENRIQUECIMENTO)
-- =============================

CREATE TABLE IF NOT EXISTS raw.fornecedores_unicos (
    id_fornecedor_unico BIGSERIAL PRIMARY KEY,
    fornecedor_original TEXT NOT NULL UNIQUE,
    fornecedor_limpo TEXT NOT NULL,
    hash_fornecedor TEXT NOT NULL,
    criado_em TIMESTAMP DEFAULT NOW(),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fornecedores_unicos_hash
    ON raw.fornecedores_unicos (hash_fornecedor);

CREATE TABLE IF NOT EXISTS raw.fornecedores_normalizados (
    hash_fornecedor TEXT PRIMARY KEY,
    fornecedor_original TEXT NOT NULL,
    fornecedor_normalizado TEXT NOT NULL,
    grupo_deduplicacao TEXT,
    cnpj_sugerido_llm VARCHAR(14),
    confianca_llm NUMERIC(5,2),
    modelo_llm TEXT,
    status_normalizacao VARCHAR(30) NOT NULL DEFAULT 'pendente',
    observacao TEXT,
    processado_em TIMESTAMP,
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fornecedores_normalizados_status
    ON raw.fornecedores_normalizados (status_normalizacao);

CREATE TABLE IF NOT EXISTS dw.fornecedores_enriquecidos (
    id_fornecedor BIGSERIAL PRIMARY KEY,
    fornecedor_normalizado TEXT NOT NULL UNIQUE,
    cnpj_sugerido_llm VARCHAR(14),
    cnpj_validado VARCHAR(14),
    razao_social TEXT,
    nome_fantasia TEXT,
    uf CHAR(2),
    municipio TEXT,
    situacao_cadastral TEXT,
    fonte_validacao VARCHAR(30),
    status_enriquecimento VARCHAR(30) NOT NULL DEFAULT 'pendente',
    confianca_llm NUMERIC(5,2),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fornecedores_enriquecidos_cnpj
    ON dw.fornecedores_enriquecidos (cnpj_validado);

CREATE TABLE IF NOT EXISTS dw.fornecedor_alias (
    fornecedor_original TEXT PRIMARY KEY,
    hash_fornecedor TEXT NOT NULL,
    fornecedor_normalizado TEXT NOT NULL,
    id_fornecedor BIGINT REFERENCES dw.fornecedores_enriquecidos(id_fornecedor),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fornecedor_alias_hash
    ON dw.fornecedor_alias (hash_fornecedor);