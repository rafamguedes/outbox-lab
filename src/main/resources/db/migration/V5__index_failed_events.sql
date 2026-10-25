-- O painel filtra as falhas antes de paginar, sem percorrer o histórico concluído.
CREATE INDEX idx_outbox_failed_recent ON outbox(created_at DESC, id)
    WHERE status = 'RETRY';
