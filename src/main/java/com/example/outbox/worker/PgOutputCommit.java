package com.example.outbox.worker;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import org.postgresql.replication.LogSequenceNumber;

final class PgOutputCommit {
    private PgOutputCommit() {}

    static Optional<LogSequenceNumber> endLsn(ByteBuffer message) {
        if (!message.hasRemaining() || message.get(message.position()) != 'C') return Optional.empty();
        // Protocolo pgoutput v1: tipo, flags, LSN do commit, LSN final e timestamp.
        if (message.remaining() != 26) throw new IllegalArgumentException("Invalid pgoutput commit frame.");
        ByteBuffer copy = message.duplicate().order(ByteOrder.BIG_ENDIAN);
        copy.get();
        if (copy.get() != 0) throw new IllegalArgumentException("Unsupported pgoutput commit flags.");
        copy.getLong();
        return Optional.of(LogSequenceNumber.valueOf(copy.getLong()));
    }
}
