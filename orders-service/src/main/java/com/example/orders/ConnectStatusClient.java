package com.example.orders;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.JsonNode;

@Component
public class ConnectStatusClient {

    private static final List<String> NAMES = List.of("orders-file-sink", "license-file-source");

    private final RestClient http;
    private final KafkaAdmin kafkaAdmin;
    private volatile String kafkaState = "UNKNOWN";
    private volatile Instant kafkaCheckedAt = Instant.EPOCH;

    public ConnectStatusClient(
            @Value("${app.connect.url}") String connectUrl,
            KafkaAdmin kafkaAdmin,
            RestClient.Builder restClientBuilder) {
        this.http = restClientBuilder.baseUrl(connectUrl).build();
        this.kafkaAdmin = kafkaAdmin;
    }

    public String kafkaState() {
        if (kafkaCheckedAt.isAfter(Instant.now().minusSeconds(5))) {
            return kafkaState;
        }
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            admin.describeCluster().clusterId().get(2, TimeUnit.SECONDS);
            kafkaState = "UP";
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            kafkaState = "DOWN";
        } catch (Exception ex) {
            kafkaState = "DOWN";
        }
        kafkaCheckedAt = Instant.now();
        return kafkaState;
    }

    public List<ConnectorStatus> connectors() {
        List<ConnectorStatus> statuses = new ArrayList<>();
        for (String name : NAMES) {
            statuses.add(status(name));
        }
        return statuses;
    }

    private ConnectorStatus status(String name) {
        try {
            JsonNode body = http.get()
                    .uri("/connectors/{name}/status", name)
                    .retrieve()
                    .body(JsonNode.class);
            if (body == null) {
                return new ConnectorStatus(name, "UNKNOWN", "UNKNOWN", 0);
            }
            String state = body.path("connector").path("state").asText("UNKNOWN");
            String type = body.path("type").asText("UNKNOWN");
            int running = 0;
            for (JsonNode task : body.path("tasks")) {
                if ("RUNNING".equals(task.path("state").asText())) {
                    running++;
                }
            }
            return new ConnectorStatus(name, type, state, running);
        } catch (RuntimeException ex) {
            return new ConnectorStatus(name, "UNKNOWN", "UNREACHABLE", 0);
        }
    }

    public record ConnectorStatus(String name, String type, String state, int runningTasks) {
    }
}
