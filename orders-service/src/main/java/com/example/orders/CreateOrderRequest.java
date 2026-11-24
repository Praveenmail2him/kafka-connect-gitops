package com.example.orders;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record CreateOrderRequest(
        @NotBlank String customer,
        @NotBlank String sku,
        @Min(1) int quantity
) {
}
