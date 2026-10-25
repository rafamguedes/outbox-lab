package com.example.outbox.repository;

import com.example.outbox.domain.Order;
import com.example.outbox.domain.OutboxEvent;
import com.example.outbox.dto.CreateOrderRequest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class OutboxStore {
    private final JdbcTemplate jdbc;

    public OutboxStore(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    private final RowMapper<Order> orderMapper = (rs, row) -> new Order(
            rs.getObject("id", UUID.class), rs.getString("product"), rs.getInt("quantity"),
            rs.getString("status"), date(rs, "created_at"), date(rs, "processed_at"));
    private final RowMapper<OutboxEvent> eventMapper = (rs, row) -> new OutboxEvent(
            rs.getObject("id", UUID.class), rs.getObject("order_id", UUID.class),
            rs.getString("type"), rs.getString("payload"), rs.getString("status"),
            rs.getInt("attempts"), rs.getInt("failures_remaining"), rs.getString("last_error"),
            date(rs, "created_at"), date(rs, "available_at"), date(rs, "processed_at"));

    private static OffsetDateTime date(ResultSet rs, String name) throws SQLException {
        return rs.getObject(name, OffsetDateTime.class);
    }

    public void insertOrder(UUID id, CreateOrderRequest order, OffsetDateTime now) {
        jdbc.update("""
                INSERT INTO orders (id, product, quantity, status, created_at)
                VALUES (?, ?, ?, 'RECEIVED', ?)
                """, id, order.product().strip(), order.quantity(), now);
    }

    public void insertEvent(UUID id, UUID orderId, String payload, int failuresRemaining, OffsetDateTime now) {
        jdbc.update("""
                INSERT INTO outbox (id, order_id, type, payload, failures_remaining, created_at, available_at)
                VALUES (?, ?, 'OrderCreated', CAST(? AS jsonb), ?, ?, CURRENT_TIMESTAMP)
                """, id, orderId, payload, failuresRemaining, now);
    }

    public Optional<Order> findOrder(UUID id) {
        return jdbc.query("SELECT * FROM orders WHERE id = ?", orderMapper, id).stream().findFirst();
    }

    public Optional<Long> nextAvailableDelayMillis() {
        return jdbc.query("""
                SELECT GREATEST(0, CEIL(EXTRACT(EPOCH FROM
                    (available_at - clock_timestamp())) * 1000))::bigint AS delay_ms
                FROM outbox WHERE status IN ('PENDING', 'RETRY')
                ORDER BY available_at, created_at, id LIMIT 1
                """, (rs, row) -> rs.getLong("delay_ms")).stream().findFirst();
    }

    public List<OutboxEvent> events() {
        return jdbc.query("SELECT * FROM outbox ORDER BY created_at DESC, id LIMIT 50", eventMapper);
    }

    public List<OutboxEvent> failedEvents(int limit, long offset) {
        return jdbc.query("""
                SELECT * FROM outbox WHERE status = 'FAILED'
                ORDER BY created_at DESC, id LIMIT ? OFFSET ?
                """, eventMapper, limit, offset);
    }

    public List<OutboxEvent> processedEvents(int limit, long offset) {
        return jdbc.query("""
                SELECT * FROM outbox WHERE status = 'PROCESSED'
                ORDER BY processed_at DESC, id LIMIT ? OFFSET ?
                """, eventMapper, limit, offset);
    }

    public boolean requeueFailedEvent(UUID id) {
        // A condição impede cliques repetidos de recolocar eventos já pendentes
        // ou concluídos. Os contadores e o último erro são preservados.
        return jdbc.update("""
                UPDATE outbox SET status = 'PENDING', available_at = clock_timestamp()
                WHERE id = ? AND status = 'FAILED'
                """, id) == 1;
    }

    // A transação mantém a linha bloqueada até concluir o processamento.
    // Outro worker ignora a linha bloqueada e pode pegar o próximo evento.
    public Optional<OutboxEvent> lockNextEvent() {
        return jdbc.query("""
                SELECT * FROM outbox
                WHERE status IN ('PENDING', 'RETRY') AND available_at <= CURRENT_TIMESTAMP
                ORDER BY available_at, created_at, id
                LIMIT 1 FOR UPDATE SKIP LOCKED
                """, eventMapper).stream().findFirst();
    }

    public void processOrder(UUID id) {
        int changed = jdbc.update("""
                UPDATE orders SET status = 'PROCESSED', processed_at = CURRENT_TIMESTAMP
                WHERE id = ? AND status = 'RECEIVED'
                """, id);
        if (changed == 0 && findOrder(id).isEmpty()) {
            throw new IllegalStateException("Event order not found.");
        }
        // Se já estiver processado, a repetição não altera a data nem repete o efeito.
    }

    public void completeEvent(UUID id) {
        jdbc.update("""
                UPDATE outbox SET status = 'PROCESSED', attempts = attempts + 1,
                    processed_at = CURRENT_TIMESTAMP, last_error = NULL
                WHERE id = ?
                """, id);
    }

    public void retryEvent(UUID id, String error, int retrySeconds) {
        jdbc.update("""
                UPDATE outbox SET status = 'RETRY', attempts = attempts + 1,
                    failures_remaining = GREATEST(failures_remaining - 1, 0),
                    last_error = ?, available_at = clock_timestamp() + (? * INTERVAL '1 second')
                WHERE id = ?
                """, error, retrySeconds, id);
    }

    public long countOrders() { return count("SELECT COUNT(*) FROM orders"); }

    public void failEvent(UUID id, String error) {
        jdbc.update("""
                UPDATE outbox SET status = 'FAILED', attempts = attempts + 1,
                    failures_remaining = GREATEST(failures_remaining - 1, 0), last_error = ?
                WHERE id = ?
                """, error, id);
    }

    public long countPending() { return count("SELECT COUNT(*) FROM outbox WHERE status IN ('PENDING', 'RETRY')"); }
    public long countProcessed() { return count("SELECT COUNT(*) FROM orders WHERE status = 'PROCESSED'"); }
    public long countRetries() { return count("SELECT COUNT(*) FROM outbox WHERE status = 'RETRY'"); }
    public long countFailures() { return count("SELECT COUNT(*) FROM outbox WHERE status = 'FAILED'"); }
    public long countProcessedEvents() { return count("SELECT COUNT(*) FROM outbox WHERE status = 'PROCESSED'"); }

    private long count(String sql) {
        Long value = jdbc.queryForObject(sql, Long.class);
        return value == null ? 0 : value;
    }
}
