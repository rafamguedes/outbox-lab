package com.example.outbox.worker;

import com.example.outbox.repository.OutboxStore;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;

public final class CdcWorker extends JdbcSignalWorker {
    private final String slotName;
    private final String publicationName;
    private LogSequenceNumber startLsn;

    public CdcWorker(OutboxRunner runner, OutboxStore store, JdbcConnectionDetails connectionDetails,
                     boolean enabled, long recoveryMillis, long reconnectMillis,
                     String slotName, String publicationName) {
        super(runner, store, connectionDetails, enabled, recoveryMillis, reconnectMillis);
        this.slotName = identifier(slotName);
        this.publicationName = identifier(publicationName);
    }

    private static String identifier(String value) {
        if (value == null || !value.matches("[a-z_][a-z0-9_]{0,62}")) {
            throw new IllegalArgumentException("CDC identifiers must contain 1-63 lowercase letters, digits, or underscores and cannot start with a digit.");
        }
        return value;
    }

    @Override public WorkerType getType() { return WorkerType.CDC; }

    @Override
    protected void beforeStart() {
        try (Connection connection = openControlConnection(); var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            try (var rows = statement.executeQuery("SHOW wal_level")) {
                if (!rows.next() || !"logical".equals(rows.getString(1))) {
                    throw new IllegalStateException("CDC requires PostgreSQL wal_level=logical. Set POSTGRES_WAL_LEVEL=logical and restart PostgreSQL.");
                }
            }
        } catch (SQLException e) { throw new IllegalStateException("Cannot validate PostgreSQL CDC configuration.", e); }
    }

    @Override
    protected Properties sourceProperties() {
        Properties properties = connectionProperties();
        properties.setProperty("replication", "database");
        properties.setProperty("preferQueryMode", "simple");
        properties.setProperty("assumeMinServerVersion", "9.4");
        return properties;
    }

    @Override
    protected void prepareSource() throws SQLException {
        try (Connection connection = openControlConnection()) {
            boolean publicationExists;
            try (var statement = connection.prepareStatement("SELECT pubinsert AND pubupdate FROM pg_publication WHERE pubname=?")) {
                statement.setQueryTimeout(5);
                statement.setString(1, publicationName);
                try (var rows = statement.executeQuery()) {
                    publicationExists = rows.next();
                    if (publicationExists && !rows.getBoolean(1)) throw new SQLException("CDC publication must publish inserts and updates.");
                }
            }
            if (!publicationExists) {
                try (var statement = connection.createStatement()) {
                    statement.setQueryTimeout(5);
                    // Os identificadores foram validados; apenas três colunas entram no stream.
                    statement.execute("CREATE PUBLICATION " + publicationName
                            + " FOR TABLE public.outbox (id, status, available_at) WITH (publish='insert, update')");
                }
            }
            try (var statement = connection.prepareStatement("SELECT 1 FROM pg_publication_tables WHERE pubname=? AND schemaname='public' AND tablename='outbox'")) {
                statement.setQueryTimeout(5);
                statement.setString(1, publicationName);
                try (var rows = statement.executeQuery()) {
                    if (!rows.next()) throw new SQLException("CDC publication does not include public.outbox.");
                }
            }
            boolean slotExists;
            try (var statement = connection.prepareStatement("SELECT plugin, database=current_database(), wal_status FROM pg_replication_slots WHERE slot_name=?")) {
                statement.setQueryTimeout(5);
                statement.setString(1, slotName);
                try (var rows = statement.executeQuery()) {
                    slotExists = rows.next();
                    if (slotExists && (!"pgoutput".equals(rows.getString(1)) || !rows.getBoolean(2))) {
                        throw new SQLException("CDC slot belongs to a different database or output plugin.");
                    }
                    if (slotExists && "lost".equals(rows.getString(3))) {
                        throw new SQLException("CDC slot lost retained WAL; recreate the inactive slot to resume CDC. Safety recovery remains enabled.");
                    }
                }
            }
            if (!slotExists) {
                try (var statement = connection.prepareStatement("SELECT * FROM pg_create_logical_replication_slot(?, 'pgoutput')")) {
                    statement.setQueryTimeout(5);
                    statement.setString(1, slotName);
                    statement.execute();
                }
            }
            try (var statement = connection.prepareStatement("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=?")) {
                statement.setQueryTimeout(5);
                statement.setString(1, slotName);
                try (var rows = statement.executeQuery()) {
                    if (!rows.next()) throw new SQLException("CDC slot disappeared before streaming.");
                    startLsn = LogSequenceNumber.valueOf(rows.getString(1));
                }
            }
        }
    }

    @Override
    protected void consume(Connection connection) throws SQLException, InterruptedException {
        PGConnection pg = connection.unwrap(PGConnection.class);
        try (var stream = pg.getReplicationAPI().replicationStream().logical()
                .withSlotName(slotName).withStartPosition(startLsn)
                .withSlotOption("proto_version", 1).withSlotOption("publication_names", publicationName)
                .withStatusInterval(1, TimeUnit.SECONDS).start()) {
            sourceReady(pg.getBackendPID());
            while (sourceActive()) {
                ByteBuffer message = stream.readPending();
                if (message == null) { Thread.sleep(100); continue; }
                var committed = PgOutputCommit.endLsn(message);
                if (committed.isPresent()) {
                    // O WAL sinaliza commits; a tabela continua sendo a fonte durável.
                    // Um crash após este ACK é coberto pela recuperação inicial da outbox.
                    signal();
                    stream.setAppliedLSN(committed.get());
                    stream.setFlushedLSN(committed.get());
                }
            }
        }
    }
}
