package com.example.orders;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import jakarta.validation.Valid;

@RestController
public class OrderController {

    private final KafkaTemplate<String, Order> kafka;
    private final RecentOrders recentOrders;
    private final String topic;

    public OrderController(
            KafkaTemplate<String, Order> kafka,
            RecentOrders recentOrders,
            @Value("${app.kafka.orders-topic}") String topic) {
        this.kafka = kafka;
        this.recentOrders = recentOrders;
        this.topic = topic;
    }

    @PostMapping("/api/orders")
    public ResponseEntity<Order> create(@Valid @RequestBody CreateOrderRequest request) {
        Order order = new Order(
                UUID.randomUUID().toString(),
                request.customer(),
                request.sku(),
                request.quantity(),
                Instant.now());
        try {
            kafka.send(topic, order.id(), order).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Interrupted while publishing order");
        } catch (ExecutionException | TimeoutException ex) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka is not ready to accept orders");
        }
        recentOrders.add(order);
        return ResponseEntity.status(HttpStatus.CREATED).body(order);
    }

    @GetMapping("/api/orders")
    public List<Order> list() {
        return recentOrders.snapshot();
    }
}
