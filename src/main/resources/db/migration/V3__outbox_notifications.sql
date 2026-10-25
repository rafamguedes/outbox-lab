CREATE FUNCTION notify_outbox_work() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- O sinal só é entregue após o commit. O payload continua na tabela.
    PERFORM pg_notify('outbox_events', '');
    RETURN NULL;
END;
$$;

-- Uma notificação por instrução, inclusive para INSERTs de 500 itens do lote.
CREATE TRIGGER outbox_insert_notify
AFTER INSERT ON outbox
FOR EACH STATEMENT EXECUTE FUNCTION notify_outbox_work();

-- Reagendamentos avisam os listeners para recalcular a próxima espera.
-- A conclusão normal do evento não altera available_at nem emite outro sinal.
CREATE TRIGGER outbox_schedule_notify
AFTER UPDATE OF available_at ON outbox
FOR EACH STATEMENT EXECUTE FUNCTION notify_outbox_work();
