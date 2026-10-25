package com.example.outbox.controller;

import com.example.outbox.domain.Order;
import com.example.outbox.domain.OutboxEvent;
import com.example.outbox.dto.CreateOrderRequest;
import com.example.outbox.service.OrderService;
import com.example.outbox.repository.OutboxStore;

import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class ApiController {
    private final OrderService service;
    private final OutboxStore store;

    public ApiController(OrderService service, OutboxStore store) {
        this.service = service;
        this.store = store;
    }

    @PostMapping("/orders")
    public ResponseEntity<Order> create(@Valid @RequestBody CreateOrderRequest request) {
        Order order = service.create(request);
        return ResponseEntity.created(URI.create("/orders/" + order.id())).body(order);
    }

    @GetMapping("/orders/{id}")
    public Order find(@PathVariable UUID id) {
        return store.findOrder(id).orElseThrow(() ->
                new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found."));
    }

    @GetMapping("/outbox")
    public List<OutboxEvent> events() { return store.events(); }
}
