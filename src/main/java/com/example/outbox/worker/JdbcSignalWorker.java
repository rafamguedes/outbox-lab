package com.example.outbox.worker;

import com.example.outbox.repository.OutboxStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;

public abstract class JdbcSignalWorker extends AbstractOutboxWorker {
    private final JdbcConnectionDetails connectionDetails;
    private final AtomicReference<Connection> sourceConnection = new AtomicReference<>();
    private volatile boolean sourceConnected;
    private volatile int sourceBackendPid;

    protected JdbcSignalWorker(OutboxRunner runner, OutboxStore store, JdbcConnectionDetails connectionDetails,
                               boolean enabled, long recoveryMillis, long reconnectMillis) {
        super(runner, store, enabled, recoveryMillis, reconnectMillis);
        this.connectionDetails = connectionDetails;
    }

    protected Properties connectionProperties() {
        Properties properties = new Properties();
        if (connectionDetails.getUsername() != null) properties.setProperty("user", connectionDetails.getUsername());
        if (connectionDetails.getPassword() != null) properties.setProperty("password", connectionDetails.getPassword());
        properties.setProperty("ApplicationName", "outbox-" + getType().name().toLowerCase(java.util.Locale.ROOT));
        properties.setProperty("connectTimeout", "5");
        properties.setProperty("socketTimeout", "5");
        properties.setProperty("tcpKeepAlive", "true");
        return properties;
    }

    protected final Connection openConnection(Properties properties) throws SQLException {
        return DriverManager.getConnection(connectionDetails.getJdbcUrl(), properties);
    }

    protected final Connection openControlConnection() throws SQLException {
        return openConnection(connectionProperties());
    }

    protected Properties sourceProperties() { return connectionProperties(); }
    protected void prepareSource() throws SQLException {}
    protected abstract void consume(Connection connection) throws SQLException, InterruptedException;

    protected final void sourceReady(int backendPid) {
        if (!sourceActive()) return;
        sourceBackendPid = backendPid;
        sourceConnected = true;
        // Registra a fonte antes da recuperação para cobrir a janela de conexão.
        signal();
        log.info("{} source connected; recovering pending events", getType().getDisplayName());
    }

    @Override protected final boolean hasSignalSource() { return true; }
    @Override protected final boolean sourceAvailable() { return sourceConnected; }
    public final boolean isSourceConnected() { return sourceConnected; }
    public final int getSourceBackendPid() { return sourceBackendPid; }

    @Override
    protected final void runSignalSource() {
        while (sourceActive()) {
            try {
                prepareSource();
                if (!sourceActive()) break;
                // A fonte tem conexão própria, fora das três vagas do Hikari.
                try (Connection connection = openConnection(sourceProperties())) {
                    sourceConnection.set(connection);
                    try {
                        if (sourceActive()) consume(connection);
                    } finally { sourceConnection.compareAndSet(connection, null); }
                }
            } catch (SQLException | RuntimeException e) {
                if (sourceActive()) log.warn("{} source unavailable; retrying in {} ms: {}",
                        getType().getDisplayName(), reconnectMillis, e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } finally {
                if (currentSourceThread()) { sourceConnected = false; sourceBackendPid = 0; }
            }
            if (sourceActive()) {
                try { Thread.sleep(reconnectMillis); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            }
        }
    }

    @Override
    protected final void closeSignalSource() {
        sourceConnected = false;
        sourceBackendPid = 0;
        Connection connection = sourceConnection.getAndSet(null);
        if (connection != null) {
            try { connection.close(); }
            catch (SQLException e) { log.debug("Source connection closed with an error", e); }
        }
    }
}
