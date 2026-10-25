-- As migrations anteriores permanecem intactas para atualizar bancos já existentes.
-- A remoção do vínculo e do histórico de lotes preserva todos os pedidos e eventos.
ALTER TABLE pedidos DROP COLUMN lote_id;
DROP TABLE lotes;

ALTER TABLE pedidos RENAME TO orders;
ALTER TABLE orders RENAME COLUMN produto TO product;
ALTER TABLE orders RENAME COLUMN quantidade TO quantity;
ALTER TABLE outbox RENAME COLUMN pedido_id TO order_id;
ALTER TABLE outbox RENAME COLUMN tipo TO type;
ALTER INDEX pedidos_pkey RENAME TO orders_pkey;
ALTER INDEX idx_pedidos_recent RENAME TO idx_orders_recent;
ALTER TABLE orders RENAME CONSTRAINT pedidos_quantidade_check TO orders_quantity_check;
ALTER TABLE outbox RENAME CONSTRAINT outbox_pedido_id_fkey TO outbox_order_id_fkey;

DROP INDEX idx_outbox_pending;
ALTER TABLE orders DROP CONSTRAINT pedidos_status_check;
ALTER TABLE outbox DROP CONSTRAINT outbox_status_check;

UPDATE orders SET status = CASE status
    WHEN 'RECEBIDO' THEN 'RECEIVED'
    WHEN 'PROCESSADO' THEN 'PROCESSED'
    ELSE status END;

-- Converte os payloads históricos sem trocar seus identificadores.
UPDATE outbox SET
    type = CASE WHEN type = 'PedidoCriado' THEN 'OrderCreated' ELSE type END,
    status = CASE status
        WHEN 'PENDENTE' THEN 'PENDING'
        WHEN 'RETENTATIVA' THEN 'RETRY'
        WHEN 'PROCESSADO' THEN 'PROCESSED'
        ELSE status END,
    payload = (payload - 'pedidoId' - 'produto' - 'quantidade') ||
        jsonb_build_object(
            'orderId', COALESCE(payload->'orderId', payload->'pedidoId'),
            'product', COALESCE(payload->'product', payload->'produto'),
            'quantity', COALESCE(payload->'quantity', payload->'quantidade'));

ALTER TABLE orders ADD CONSTRAINT orders_status_check CHECK (status IN ('RECEIVED', 'PROCESSED'));
ALTER TABLE outbox ADD CONSTRAINT outbox_status_check CHECK (status IN ('PENDING', 'RETRY', 'PROCESSED'));
ALTER TABLE outbox ALTER COLUMN status SET DEFAULT 'PENDING';
CREATE INDEX idx_outbox_pending ON outbox(available_at, created_at, id)
    WHERE status <> 'PROCESSED';
