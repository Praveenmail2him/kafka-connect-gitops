package com.example.orders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    private final RecentOrders recentOrders;

    public OrderListener(RecentOrders recentOrders) {
        this.recentOrders = recentOrders;
    }

    @KafkaListener(topics = "${app.kafka.orders-topic}", groupId = "orders-service")
    public void onOrder(Order order) {
        recentOrders.add(order);
        log.info("Consumed order {} for customer {}", order.id(), order.customer());
    }
}
