package com.example.outbox.service;

import com.example.outbox.domain.OutboxEvent;
import com.example.outbox.repository.OutboxStore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderProcessor {
    private final OutboxStore store;
    private final ObjectMapper mapper;

    public OrderProcessor(OutboxStore store, ObjectMapper mapper) {
        this.store = store;
        this.mapper = mapper;
    }

    // O savepoint permite desfazer o pedido e registrar a falha na transação externa.
    @Transactional(propagation = Propagation.NESTED)
    public void process(OutboxEvent event) {
        if (!"OrderCreated".equals(event.type())) {
            throw new IllegalArgumentException("Unsupported event type: " + event.type());
        }
        OrderService.OrderCreated payload;
        try {
            payload = mapper.readValue(event.payload(), OrderService.OrderCreated.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid event payload.", e);
        }
        if (!event.orderId().equals(payload.orderId())) {
            throw new IllegalArgumentException("Payload order does not match the event.");
        }
        store.processOrder(payload.orderId());
        // A falha ocorre após o UPDATE para demonstrar o rollback do pedido.
        if (event.failuresRemaining() > 0) {
            throw new IllegalStateException("Simulated failure: the order UPDATE was rolled back.");
        }
    }
}
