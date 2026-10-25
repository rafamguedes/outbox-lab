package com.example.outbox.worker;

import com.example.outbox.repository.OutboxStore;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

class PollingWorkerTest {
    private final OutboxRunner runner = mock(OutboxRunner.class);
    private final OutboxStore store = mock(OutboxStore.class);
    private PollingWorker worker;

    @BeforeEach
    void setUp() { worker = new PollingWorker(runner, store, true, 60, 30); }

    @AfterEach
    void stop() { worker.stop(); }

    @Test
    void pollsWithoutAnyExternalSignalAndStopsPollingAfterShutdown() {
        worker.start();
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> verify(runner, atLeast(3)).processNext());
        worker.stop();
        int calls = calls();
        await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(calls()).isEqualTo(calls));
        assertThat(worker.isRunning()).isFalse();
        verifyNoInteractions(store);
    }

    @Test
    void pauseSuppressesQueriesAndResumeWakesProcessingImmediately() {
        worker = new PollingWorker(runner, store, false, 10000, 30);
        worker.start();
        await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> verifyNoInteractions(runner, store));
        worker.setEnabled(true);
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> verify(runner).processNext());
    }

    @Test
    void aSingleWakeupDrainsMoreThanOneChunk() {
        worker = new PollingWorker(runner, store, true, 10000, 30);
        AtomicInteger attempts = new AtomicInteger();
        when(runner.processNext()).thenAnswer(invocation -> attempts.getAndIncrement() < 105);
        worker.start();
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(attempts.get()).isEqualTo(106));
        verify(runner, times(106)).processNext();
    }

    @Test
    void infrastructureFailureRetriesBeforeTheNormalPollingInterval() {
        worker = new PollingWorker(runner, store, true, 10000, 30);
        when(runner.processNext()).thenThrow(new IllegalStateException("Database temporarily unavailable")).thenReturn(false);
        worker.start();
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> verify(runner, times(2)).processNext());
    }

    @Test
    void repeatedStartDoesNotDuplicateThreadsAndRestartRecoversPendingWork() {
        worker = new PollingWorker(runner, store, true, 10000, 30);
        worker.start();
        worker.start();
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> verify(runner, times(1)).processNext());
        worker.stop();
        worker.start();
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> verify(runner, times(2)).processNext());
    }

    private int calls() {
        return (int) mockingDetails(runner).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("processNext")).count();
    }
}
