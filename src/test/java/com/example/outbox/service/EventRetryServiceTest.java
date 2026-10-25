package com.example.outbox.service;

import com.example.outbox.repository.OutboxStore;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventRetryServiceTest {
    @Mock OutboxStore store;
    private EventRetryService service;
    private final UUID eventId = UUID.randomUUID();

    @BeforeEach
    void setUp() { service = new EventRetryService(store); }

    @Test
    void queuesOnlyTheSelectedPermanentFailureWithoutProcessingIt() {
        when(store.requeueFailedEvent(eventId)).thenReturn(true);
        assertThat(service.retryFailedEvent(eventId)).isTrue();
        verify(store).requeueFailedEvent(eventId);
        verifyNoMoreInteractions(store);
    }

    @Test
    void refusesEventsThatAreNoLongerPermanentlyFailed() {
        when(store.requeueFailedEvent(eventId)).thenReturn(false);
        assertThat(service.retryFailedEvent(eventId)).isFalse();
        verify(store).requeueFailedEvent(eventId);
        verifyNoMoreInteractions(store);
    }

    @Test
    void repositoryFailureIsPropagatedInsteadOfReportingSuccess() {
        var failure = new IllegalStateException("Cannot queue");
        when(store.requeueFailedEvent(eventId)).thenThrow(failure);
        assertThatThrownBy(() -> service.retryFailedEvent(eventId)).isSameAs(failure);
        verify(store).requeueFailedEvent(eventId);
        verifyNoMoreInteractions(store);
    }
}
