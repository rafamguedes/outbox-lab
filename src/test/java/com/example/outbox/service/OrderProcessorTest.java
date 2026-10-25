package com.example.outbox.service;

import com.example.outbox.domain.OutboxEvent;
import com.example.outbox.repository.OutboxStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderProcessorTest {
    @Mock OutboxStore store;
    private final ObjectMapper mapper = new ObjectMapper();
    private OrderProcessor processor;
    private final UUID orderId = UUID.randomUUID();

    @BeforeEach
    void setUp() { processor = new OrderProcessor(store, mapper); }

    @Test
    void validPayloadProcessesTheReferencedOrder() throws Exception {
        processor.process(event("OrderCreated", payload(orderId), 0));
        verify(store).processOrder(orderId);
        verifyNoMoreInteractions(store);
    }

    @Test
    void unsupportedTypeHasNoOrderEffect() throws Exception {
        assertThatThrownBy(() -> processor.process(event("UnknownEvent", payload(orderId), 0)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported event type");
        verifyNoInteractions(store);
    }

    @Test
    void malformedPayloadHasNoOrderEffect() {
        assertThatThrownBy(() -> processor.process(event("OrderCreated", "{invalid", 0)))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("Invalid event payload.")
                .hasCauseInstanceOf(com.fasterxml.jackson.core.JsonProcessingException.class);
        verifyNoInteractions(store);
    }

    @Test
    void mismatchedOrderIdHasNoOrderEffect() throws Exception {
        String differentOrderPayload = payload(UUID.randomUUID());
        assertThatThrownBy(() -> processor.process(event("OrderCreated", differentOrderPayload, 0)))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("Payload order does not match the event.");
        verifyNoInteractions(store);
    }

    @Test
    void simulatedFailureOccursAfterTheOrderUpdate() throws Exception {
        var event = event("OrderCreated", payload(orderId), 1);
        assertThatThrownBy(() -> processor.process(event))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Simulated failure");
        verify(store).processOrder(orderId);
        verifyNoMoreInteractions(store);
    }

    private String payload(UUID id) throws Exception {
        return mapper.writeValueAsString(new OrderService.OrderCreated(id, "Brake pad", 2));
    }

    private OutboxEvent event(String type, String payload, int failuresRemaining) {
        OffsetDateTime now = OffsetDateTime.now();
        return new OutboxEvent(UUID.randomUUID(), orderId, type, payload, "PENDING", 0,
                failuresRemaining, null, now, now, null);
    }
}
