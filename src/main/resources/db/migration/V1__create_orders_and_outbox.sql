CREATE TABLE pedidos (
    id UUID PRIMARY KEY,
    produto VARCHAR(120) NOT NULL,
    quantidade INTEGER NOT NULL CHECK (quantidade > 0),
    status VARCHAR(20) NOT NULL CHECK (status IN ('RECEBIDO', 'PROCESSADO')),
    created_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ
);

CREATE TABLE outbox (
    id UUID PRIMARY KEY,
    pedido_id UUID NOT NULL REFERENCES pedidos(id),
    tipo VARCHAR(60) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDENTE'
        CHECK (status IN ('PENDENTE', 'RETENTATIVA', 'PROCESSADO')),
    attempts INTEGER NOT NULL DEFAULT 0,
    failures_remaining INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_pending ON outbox (available_at, created_at)
    WHERE status <> 'PROCESSADO';
