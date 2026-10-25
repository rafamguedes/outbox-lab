package com.example.outbox.service;

import com.example.outbox.domain.Order;
import com.example.outbox.dto.CreateOrderRequest;
import com.example.outbox.repository.OutboxStore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {
    @Mock OutboxStore store;
    private final ObjectMapper mapper = new ObjectMapper();
    private OrderService service;

    @BeforeEach
    void setUp() { service = new OrderService(store, mapper); }

    @Test
    void createsMatchingOrderAndEventWithNormalizedPayload() throws Exception {
        var request = new CreateOrderRequest("  Brake pad  ", 3, true, 3);
        Order result = service.create(request);
        var eventId = ArgumentCaptor.forClass(UUID.class);
        var payload = ArgumentCaptor.forClass(String.class);
        var calls = inOrder(store);
        calls.verify(store).insertOrder(result.id(), request, result.createdAt());
        calls.verify(store).insertEvent(eventId.capture(), eq(result.id()), payload.capture(),
                eq(3), eq(result.createdAt()));
        calls.verifyNoMoreInteractions();

        assertThat(eventId.getValue()).isNotEqualTo(result.id());
        assertThat(result.product()).isEqualTo("Brake pad");
        assertThat(result.quantity()).isEqualTo(3);
        assertThat(result.status()).isEqualTo("RECEIVED");
        assertThat(result.createdAt()).isNotNull();
        assertThat(result.processedAt()).isNull();
        // A serialização é real: o teste confere o contrato, além das chamadas ao repositório.
        var json = mapper.readTree(payload.getValue());
        assertThat(json.size()).isEqualTo(3);
        assertThat(json.path("orderId").asText()).isEqualTo(result.id().toString());
        assertThat(json.path("product").asText()).isEqualTo("Brake pad");
        assertThat(json.path("quantity").asInt()).isEqualTo(3);
    }

    @Test
    void serializationFailurePreventsBothInserts() throws Exception {
        ObjectMapper failingMapper = mock(ObjectMapper.class);
        var failure = new JsonProcessingException("Cannot serialize") {};
        when(failingMapper.writeValueAsString(any(OrderService.OrderCreated.class))).thenThrow(failure);
        var failingService = new OrderService(store, failingMapper);

        assertThatThrownBy(() -> failingService.create(new CreateOrderRequest("Pad", 1, false)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Could not serialize the event.").hasCause(failure);
        verifyNoInteractions(store);
    }

    @Test
    void eventInsertFailureIsPropagatedAfterOrderInsert() {
        var failure = new IllegalStateException("Event insert failed");
        doThrow(failure).when(store).insertEvent(any(), any(), anyString(), eq(0), any());

        assertThatThrownBy(() -> service.create(new CreateOrderRequest("Pad", 1, false))).isSameAs(failure);
        var calls = inOrder(store);
        calls.verify(store).insertOrder(any(UUID.class), any(CreateOrderRequest.class), any(OffsetDateTime.class));
        calls.verify(store).insertEvent(any(UUID.class), any(UUID.class), anyString(), eq(0), any(OffsetDateTime.class));
        calls.verifyNoMoreInteractions();
        // O rollback não é provado por mocks; existe um teste separado com PostgreSQL real.
    }

    @Test
    void orderInsertFailurePreventsEventInsert() {
        var failure = new IllegalStateException("Order insert failed");
        doThrow(failure).when(store).insertOrder(any(), any(), any());

        assertThatThrownBy(() -> service.create(new CreateOrderRequest("Pad", 1, false))).isSameAs(failure);
        verify(store).insertOrder(any(), any(), any());
        verifyNoMoreInteractions(store);
    }
}
