-- Habilita a extensão de criptografia para o MD5 nativo
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS empenhos_macapa (
    id_empenho VARCHAR(50) PRIMARY KEY,
    num_empenho VARCHAR(50),
    orgao TEXT,
    fornecedor TEXT,
    data_iso DATE,
    historico TEXT,
    empenhado NUMERIC(15,2),
    liquidado NUMERIC(15,2),
    gasto NUMERIC(15,2),
    -- Coluna que armazena a "assinatura" da linha
    row_hash TEXT,
    data_versao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);