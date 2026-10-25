package com.example.outbox.domain;


import java.time.OffsetDateTime;
import java.util.UUID;

public record OutboxEvent(UUID id, UUID orderId, String type, String payload, String status,
                          int attempts, int failuresRemaining, String lastError,
                          OffsetDateTime createdAt, OffsetDateTime availableAt,
                          OffsetDateTime processedAt) {}
