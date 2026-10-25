CREATE TABLE lotes (
    id UUID PRIMARY KEY,
    produto VARCHAR(100) NOT NULL,
    total INTEGER NOT NULL CHECK (total BETWEEN 1 AND 100000),
    generated INTEGER NOT NULL DEFAULT 0 CHECK (generated BETWEEN 0 AND total),
    status VARCHAR(20) NOT NULL DEFAULT 'GERANDO'
        CHECK (status IN ('GERANDO', 'CONCLUIDO', 'FALHOU')),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ
);

-- Apenas um gerador ativo, inclusive com mais de uma instância da aplicação.
CREATE UNIQUE INDEX idx_lotes_one_active ON lotes ((1)) WHERE status = 'GERANDO';
ALTER TABLE pedidos ADD COLUMN lote_id UUID REFERENCES lotes(id);
CREATE INDEX idx_pedidos_lote ON pedidos(lote_id) WHERE lote_id IS NOT NULL;
CREATE INDEX idx_pedidos_recent ON pedidos(created_at DESC, id);
CREATE INDEX idx_outbox_recent ON outbox(created_at DESC, id);
DROP INDEX idx_outbox_pending;
CREATE INDEX idx_outbox_pending ON outbox(available_at, created_at, id)
    WHERE status <> 'PROCESSADO';
