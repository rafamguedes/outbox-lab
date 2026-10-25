package com.example.outbox.dto;


import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateOrderRequest(
        @NotBlank(message = "Product is required.")
        @Size(max = 120, message = "Product must contain at most 120 characters.") String product,
        @Min(value = 1, message = "Quantity must be at least 1.")
        @Max(value = 999, message = "Quantity must be at most 999.") int quantity,
        boolean simulateFailure,
        @Min(value = 0, message = "Failure attempts must be at least 0.")
        @Max(value = 100, message = "Failure attempts must be at most 100.") int failureAttempts) {
    public CreateOrderRequest(String product, int quantity, boolean simulateFailure) {
        this(product, quantity, simulateFailure, 1);
    }

    public int failuresToSimulate() {
        return simulateFailure ? Math.max(1, failureAttempts) : 0;
    }
}
