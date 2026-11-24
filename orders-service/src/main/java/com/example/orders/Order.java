package com.example.orders;

import java.time.Instant;

public record Order(String id, String customer, String sku, int quantity, Instant createdAt) {
}
