package com.example.orders;

import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FlowController {

    private final ConnectStatusClient connect;
    private final RecentOrders recentOrders;
    private final String ordersTopic;

    public FlowController(
            ConnectStatusClient connect,
            RecentOrders recentOrders,
            @Value("${app.kafka.orders-topic}") String ordersTopic) {
        this.connect = connect;
        this.recentOrders = recentOrders;
        this.ordersTopic = ordersTopic;
    }

    @GetMapping("/api/flow")
    public FlowSnapshot flow() {
        return new FlowSnapshot(connect.kafkaState(), ordersTopic, connect.connectors(), recentOrders.snapshot());
    }

    public record FlowSnapshot(
            String kafka,
            String ordersTopic,
            List<ConnectStatusClient.ConnectorStatus> connectors,
            List<Order> orders) {
    }
}
