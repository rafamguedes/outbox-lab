package com.example.outbox.service;

import com.example.outbox.domain.Order;
import com.example.outbox.dto.CreateOrderRequest;
import com.example.outbox.repository.OutboxStore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {
    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private final OutboxStore store;
    private final ObjectMapper mapper;

    public OrderService(OutboxStore store, ObjectMapper mapper) {
        this.store = store;
        this.mapper = mapper;
    }

    @Transactional
    public Order create(CreateOrderRequest request) {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        String product = request.product().strip();
        String payload;
        try {
            payload = mapper.writeValueAsString(new OrderCreated(orderId, product, request.quantity()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize the event.", e);
        }
        // As duas inserções usam a mesma transação e conexão.
        store.insertOrder(orderId, request, now);
        store.insertEvent(eventId, orderId, payload, request.failuresToSimulate(), now);
        log.info("Order {} and event {} stored in the same transaction", orderId, eventId);
        return new Order(orderId, product, request.quantity(), "RECEIVED", now, null);
    }

    public record OrderCreated(UUID orderId, String product, int quantity) {}
}
