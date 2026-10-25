package com.example.outbox.worker;

import com.example.outbox.domain.OutboxEvent;
import com.example.outbox.repository.OutboxStore;
import com.example.outbox.service.OrderProcessor;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxRunnerTest {
    @Mock OutboxStore store;
    @Mock OrderProcessor processor;
    private OutboxRunner runner;
    private OutboxEvent event;

    @BeforeEach
    void setUp() {
        runner = new OutboxRunner(store, processor, 12, 2);
        OffsetDateTime now = OffsetDateTime.now();
        event = new OutboxEvent(UUID.randomUUID(), UUID.randomUUID(), "OrderCreated", "{}",
                "PENDING", 0, 0, null, now, now, null);
    }

    @Test
    void emptyQueueDoesNotProcessCompleteOrRetryAnything() {
        when(store.lockNextEvent()).thenReturn(Optional.empty());
        assertThat(runner.processNext()).isFalse();
        verify(store).lockNextEvent();
        verifyNoMoreInteractions(store);
        verifyNoInteractions(processor);
    }

    @Test
    void successProcessesBeforeCompletingTheSameEvent() {
        when(store.lockNextEvent()).thenReturn(Optional.of(event));
        assertThat(runner.processNext()).isTrue();
        var calls = inOrder(store, processor);
        calls.verify(store).lockNextEvent();
        calls.verify(processor).process(event);
        calls.verify(store).completeEvent(event.id());
        calls.verifyNoMoreInteractions();
    }

    @Test
    void processorFailureSchedulesRetryAndDoesNotCompleteTheEvent() {
        when(store.lockNextEvent()).thenReturn(Optional.of(event));
        doThrow(new IllegalArgumentException("Invalid payload")).when(processor).process(event);

        assertThat(runner.processNext()).isTrue();
        var calls = inOrder(store, processor);
        calls.verify(store).lockNextEvent();
        calls.verify(processor).process(event);
        calls.verify(store).retryEvent(event.id(), "Invalid payload", 12);
        calls.verifyNoMoreInteractions();
    }

    @Test
    void exceptionWithoutMessageUsesItsClassNameForTheRetry() {
        when(store.lockNextEvent()).thenReturn(Optional.of(event));
        doThrow(new IllegalStateException()).when(processor).process(event);

        assertThat(runner.processNext()).isTrue();
        verify(store).retryEvent(event.id(), "IllegalStateException", 12);
        verify(store, never()).completeEvent(any());
    }

    @Test
    void completionFailureEscapesWithoutBeingConvertedIntoAProcessingRetry() {
        when(store.lockNextEvent()).thenReturn(Optional.of(event));
        var failure = new IllegalStateException("Cannot complete event");
        doThrow(failure).when(store).completeEvent(event.id());

        assertThatThrownBy(runner::processNext).isSameAs(failure);
        var calls = inOrder(store, processor);
        calls.verify(store).lockNextEvent();
        calls.verify(processor).process(event);
        calls.verify(store).completeEvent(event.id());
        calls.verifyNoMoreInteractions();
    }

    @Test
    void lockFailureEscapesWithoutCallingTheProcessor() {
        var failure = new IllegalStateException("Database unavailable");
        when(store.lockNextEvent()).thenThrow(failure);

        assertThatThrownBy(runner::processNext).isSameAs(failure);
        verify(store).lockNextEvent();
        verifyNoMoreInteractions(store);
        verifyNoInteractions(processor);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5})
    void exhaustedAttemptsBecomePermanentIncludingFailuresAfterManualRetry(int previousAttempts) {
        var exhausted = withAttempts(previousAttempts);
        when(store.lockNextEvent()).thenReturn(Optional.of(exhausted));
        doThrow(new IllegalStateException("Still failing")).when(processor).process(exhausted);

        assertThat(runner.processNext()).isTrue();
        var calls = inOrder(store, processor);
        calls.verify(store).lockNextEvent();
        calls.verify(processor).process(exhausted);
        calls.verify(store).failEvent(exhausted.id(), "Still failing");
        calls.verifyNoMoreInteractions();
    }

    @Test
    void failureToPersistPermanentStatusIsNotReportedAsSuccess() {
        var exhausted = withAttempts(1);
        when(store.lockNextEvent()).thenReturn(Optional.of(exhausted));
        doThrow(new IllegalStateException("Processing failed")).when(processor).process(exhausted);
        var databaseFailure = new IllegalStateException("Cannot persist failure");
        doThrow(databaseFailure).when(store).failEvent(exhausted.id(), "Processing failed");
        assertThatThrownBy(runner::processNext).isSameAs(databaseFailure);
        verify(store, never()).retryEvent(any(), anyString(), anyInt());
        verify(store, never()).completeEvent(any());
    }

    private OutboxEvent withAttempts(int attempts) {
        return new OutboxEvent(event.id(), event.orderId(), event.type(), event.payload(), "RETRY",
                attempts, 1, "Previous error", event.createdAt(), event.availableAt(), null);
    }
}
