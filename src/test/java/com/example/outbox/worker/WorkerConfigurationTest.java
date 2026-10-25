package com.example.outbox.worker;

import com.example.outbox.repository.OutboxStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class WorkerConfigurationTest {
    private final OutboxRunner runner = mock(OutboxRunner.class);
    private final OutboxStore store = mock(OutboxStore.class);
    private final JdbcConnectionDetails details = mock(JdbcConnectionDetails.class);

    @ParameterizedTest
    @EnumSource(WorkerType.class)
    void constructsExactlyTheRequestedImplementationWithoutOpeningConnections(WorkerType type) {
        var worker = new WorkerConfiguration().outboxWorker(runner, store, details, type,
                true, 1000, 60000, 2000, "test_slot", "test_publication");
        Class<?> expected = switch (type) {
            case POLLING -> PollingWorker.class;
            case LISTEN_NOTIFY -> ListenNotifyWorker.class;
            case CDC -> CdcWorker.class;
        };
        assertThat(worker).isExactlyInstanceOf(expected);
        assertThat(worker.getType()).isEqualTo(type);
        assertThat(worker.isRunning()).isFalse();
        verifyNoInteractions(runner, store, details);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "wrong-name", "UPPERCASE", "slot;DROP TABLE outbox", "1slot"})
    void rejectsUnsafeCdcIdentifiersBeforeOpeningConnections(String name) {
        assertThatThrownBy(() -> new CdcWorker(runner, store, details, true, 60000, 2000, name, "safe_publication"))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(details);
    }

    @Test
    void refusesInvalidPollingIntervals() {
        assertThatThrownBy(() -> new PollingWorker(runner, store, true, 0, 2000))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
