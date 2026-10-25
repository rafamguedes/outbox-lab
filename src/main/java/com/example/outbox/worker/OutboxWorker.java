package com.example.outbox.worker;

import com.example.outbox.repository.OutboxStore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

@Component
public class OutboxWorker implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(OutboxWorker.class);
    private final OutboxRunner runner;
    private final OutboxStore store;
    private final JdbcConnectionDetails connectionDetails;
    private final AtomicBoolean enabled;
    // Um único sinal "há trabalho", nunca uma lista de eventos ou tarefas por item.
    private final ArrayBlockingQueue<Boolean> wakeups = new ArrayBlockingQueue<>(1);
    private final long recoveryMillis;
    private final long reconnectMillis;
    private volatile boolean running;
    private volatile boolean listenerConnected;
    private volatile int listenerBackendPid;
    private volatile String state = "STARTING";
    private Thread listenerThread;
    private Thread processingThread;

    public OutboxWorker(OutboxRunner runner, OutboxStore store, JdbcConnectionDetails connectionDetails,
                        @Value("${outbox.worker.enabled:true}") boolean enabled,
                        @Value("${outbox.worker.recovery-ms:60000}") long recoveryMillis,
                        @Value("${outbox.worker.reconnect-ms:2000}") long reconnectMillis) {
        this.runner = runner;
        this.store = store;
        this.connectionDetails = connectionDetails;
        this.enabled = new AtomicBoolean(enabled);
        this.recoveryMillis = Math.max(1, recoveryMillis);
        this.reconnectMillis = Math.max(1, reconnectMillis);
    }

    @Override
    public synchronized void start() {
        if (running) return;
        running = true;
        processingThread = Thread.ofPlatform().daemon().name("outbox-processing").start(this::processLoop);
        listenerThread = Thread.ofPlatform().daemon().name("outbox-listener").start(this::listenLoop);
    }

    private void listenLoop() {
        while (running) {
            try (Connection connection = openListenerConnection()) {
                connection.setAutoCommit(true);
                try (var statement = connection.createStatement()) {
                    statement.execute("LISTEN outbox_events");
                }
                PGConnection pg = connection.unwrap(PGConnection.class);
                listenerBackendPid = pg.getBackendPID();
                listenerConnected = true;
                log.info("LISTEN outbox_events connected; recovering pending events");
                // O LISTEN está confirmado antes da consulta inicial. Isso cobre
                // o início da aplicação e notificações perdidas na desconexão.
                signal();
                while (running) {
                    // Espera no socket, sem SELECT na outbox. O timeout permite
                    // encerrar a thread em aproximadamente 1 segundo.
                    var notifications = pg.getNotifications(1000);
                    if (notifications != null && notifications.length > 0) signal();
                }
            } catch (SQLException e) {
                if (running) log.warn("Listener disconnected; reconnecting in {} ms: {}",
                        reconnectMillis, e.getMessage());
            } finally {
                listenerConnected = false;
                listenerBackendPid = 0;
            }
            if (running) {
                try {
                    Thread.sleep(reconnectMillis);
                } catch (InterruptedException e) {
                    if (!running) break;
                }
            }
        }
    }

    private Connection openListenerConnection() throws SQLException {
        Properties properties = new Properties();
        if (connectionDetails.getUsername() != null) {
            properties.setProperty("user", connectionDetails.getUsername());
        }
        if (connectionDetails.getPassword() != null) {
            properties.setProperty("password", connectionDetails.getPassword());
        }
        properties.setProperty("ApplicationName", "outbox-listener");
        properties.setProperty("connectTimeout", "5");
        properties.setProperty("tcpKeepAlive", "true");
        // Conexão própria: não ocupa permanentemente uma das 3 vagas do Hikari.
        return DriverManager.getConnection(connectionDetails.getJdbcUrl(), properties);
    }

    private void processLoop() {
        long waitMillis = recoveryMillis;
        String waitingState = "WAITING_FOR_SIGNAL";
        while (running) {
            try {
                if (enabled.get()) {
                    state = waitingState;
                    wakeups.poll(waitMillis, TimeUnit.MILLISECONDS);
                } else {
                    state = "PAUSED";
                    wakeups.take();
                }
                if (!running) break;
                if (!enabled.get()) continue;
                state = "PROCESSING";
                drain();
                if (!running || !enabled.get()) continue;
                var next = store.nextAvailableDelayMillis();
                waitMillis = recoveryMillis;
                waitingState = "WAITING_FOR_SIGNAL";
                if (next.isPresent()) {
                    // O atraso é calculado no banco, sem comparar relógios de máquinas diferentes.
                    long untilNext = next.get();
                    // Linhas bloqueadas por outra instância não devem provocar
                    // um loop imediato de consultas quando já estão disponíveis.
                    waitMillis = Math.min(recoveryMillis, Math.max(1000, untilNext));
                    waitingState = untilNext > 0 ? "WAITING_FOR_RETRY" : "WAITING_FOR_AVAILABILITY";
                }
            } catch (InterruptedException e) {
                if (!running) break;
            } catch (RuntimeException e) {
                log.error("Processing failed; retrying after the recovery interval", e);
                waitMillis = Math.min(recoveryMillis, reconnectMillis);
                waitingState = "RECOVERING";
            }
        }
        state = "STOPPED";
    }

    private void drain() {
        boolean empty = false;
        while (running && enabled.get() && !empty) {
            int attempted = 0;
            long deadline = System.nanoTime() + 500_000_000L;
            for (int i = 0; i < 100 && running && enabled.get() && System.nanoTime() < deadline; i++) {
                if (!runner.processNext()) {
                    empty = true;
                    break;
                }
                attempted++;
            }
            if (attempted > 0) log.info("Worker executed {} attempt(s) in this chunk", attempted);
            // Atingir o limite do bloco continua o trabalho, sem exigir outro NOTIFY.
            // Cada evento mantém sua transação e a pausa é conferida entre eventos.
            if (!empty) Thread.yield();
        }
    }

    private void signal() { wakeups.offer(Boolean.TRUE); }

    public boolean isEnabled() { return enabled.get(); }
    public void setEnabled(boolean value) {
        enabled.set(value);
        signal(); // Ativar recupera pendências mesmo sem novos eventos.
    }
    public boolean isListenerConnected() { return listenerConnected; }
    public String getState() { return enabled.get() ? state : "PAUSED"; }
    public int getListenerBackendPid() { return listenerBackendPid; }

    public String description() {
        if (!enabled.get()) return "Automatic processing paused";
        if (!listenerConnected) return "Listener reconnecting · automatic recovery enabled";
        return switch (state) {
            case "PROCESSING" -> "LISTEN/NOTIFY · Processing events";
            case "WAITING_FOR_RETRY" -> "LISTEN/NOTIFY · Waiting for the retry time";
            case "WAITING_FOR_AVAILABILITY" -> "LISTEN/NOTIFY · Waiting for events held by another transaction";
            case "RECOVERING" -> "LISTEN/NOTIFY · Recovering after a failure";
            default -> "LISTEN/NOTIFY · Waiting for a notification";
        };
    }

    @Override
    public synchronized void stop() {
        running = false;
        signal();
        if (processingThread != null) processingThread.interrupt();
        if (listenerThread != null) listenerThread.interrupt();
        join(processingThread);
        join(listenerThread);
    }

    private void join(Thread thread) {
        if (thread == null || thread == Thread.currentThread()) return;
        try {
            thread.join(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() { return running; }
}
