package com.example.outbox.worker;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

class PgOutputCommitTest {
    @Test
    void extractsTheTransactionEndLsnWithoutChangingBufferPosition() {
        long end = 0x1122334455667788L;
        ByteBuffer buffer = ByteBuffer.allocate(28);
        buffer.putShort((short) 99).put((byte) 'C').put((byte) 0)
                .putLong(42).putLong(end).putLong(123456);
        buffer.flip().position(2);
        assertThat(PgOutputCommit.endLsn(buffer).orElseThrow().asLong()).isEqualTo(end);
        assertThat(buffer.position()).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(strings = {"B", "I", "U", "R", "Y", "D", "T", ""})
    void rowAndMetadataMessagesAreNeverTreatedAsCommits(String type) {
        assertThat(PgOutputCommit.endLsn(ByteBuffer.wrap(type.getBytes(java.nio.charset.StandardCharsets.US_ASCII)))).isEmpty();
    }

    @Test
    void rejectsATruncatedCommitInsteadOfAcknowledgingIt() {
        ByteBuffer buffer = ByteBuffer.allocate(25).put((byte) 'C');
        buffer.flip();
        assertThatThrownBy(() -> PgOutputCommit.endLsn(buffer)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsUnsupportedCommitFlags() {
        ByteBuffer buffer = ByteBuffer.allocate(26).put((byte) 'C').put((byte) 1);
        buffer.position(0);
        assertThatThrownBy(() -> PgOutputCommit.endLsn(buffer)).isInstanceOf(IllegalArgumentException.class);
    }
}
