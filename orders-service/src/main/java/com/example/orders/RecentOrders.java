package com.example.orders;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.springframework.stereotype.Component;

@Component
public class RecentOrders {

    static final int LIMIT = 100;

    private final Deque<Order> orders = new ConcurrentLinkedDeque<>();

    public void add(Order order) {
        synchronized (orders) {
            for (Order existing : orders) {
                if (existing.id().equals(order.id())) {
                    return;
                }
            }
            orders.addFirst(order);
            while (orders.size() > LIMIT) {
                orders.removeLast();
            }
        }
    }

    public List<Order> snapshot() {
        synchronized (orders) {
            return List.copyOf(new ArrayList<>(orders));
        }
    }
}
