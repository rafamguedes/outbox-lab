ALTER TABLE outbox DROP CONSTRAINT outbox_status_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_status_check
    CHECK (status IN ('PENDING', 'RETRY', 'PROCESSED', 'FAILED'));

-- Falhas permanentes deixam de participar da consulta e do timer do worker.
DROP INDEX idx_outbox_pending;
CREATE INDEX idx_outbox_pending ON outbox(available_at, created_at, id)
    WHERE status IN ('PENDING', 'RETRY');
DROP INDEX idx_outbox_failed_recent;
CREATE INDEX idx_outbox_failed_recent ON outbox(created_at DESC, id)
    WHERE status = 'FAILED';
CREATE INDEX idx_outbox_processed_recent ON outbox(processed_at DESC, id)
    WHERE status = 'PROCESSED';
