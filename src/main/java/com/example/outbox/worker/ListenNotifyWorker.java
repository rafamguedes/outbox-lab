package com.example.outbox.worker;

import com.example.outbox.repository.OutboxStore;
import java.sql.Connection;
import java.sql.SQLException;
import org.postgresql.PGConnection;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;

public final class ListenNotifyWorker extends JdbcSignalWorker {
    public ListenNotifyWorker(OutboxRunner runner, OutboxStore store, JdbcConnectionDetails connectionDetails,
                              boolean enabled, long recoveryMillis, long reconnectMillis) {
        super(runner, store, connectionDetails, enabled, recoveryMillis, reconnectMillis);
    }

    @Override public WorkerType getType() { return WorkerType.LISTEN_NOTIFY; }

    @Override
    protected void consume(Connection connection) throws SQLException {
        connection.setAutoCommit(true);
        try (var statement = connection.createStatement()) { statement.execute("LISTEN outbox_events"); }
        PGConnection pg = connection.unwrap(PGConnection.class);
        sourceReady(pg.getBackendPID());
        while (sourceActive()) {
            // Espera no socket; o timeout não executa SELECT na tabela outbox.
            var notifications = pg.getNotifications(1000);
            if (notifications != null && notifications.length > 0) signal();
        }
    }
}
