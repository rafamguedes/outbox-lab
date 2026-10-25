package com.example.outbox.domain;


import java.time.OffsetDateTime;
import java.util.UUID;

public record Order(UUID id, String product, int quantity, String status,
                    OffsetDateTime createdAt, OffsetDateTime processedAt) {}
