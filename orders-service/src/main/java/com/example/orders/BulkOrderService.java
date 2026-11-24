package com.example.orders;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PreDestroy;

@Service
public class BulkOrderService {

    public static final long MAX_COUNT = 10_000_000_000L;
    private static final int MAX_IN_FLIGHT = 2_000;

    private final KafkaTemplate<String, Order> kafka;
    private final String topic;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "bulk-orders");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicReference<Job> active = new AtomicReference<>();

    public BulkOrderService(
            KafkaProperties properties,
            ObjectMapper objectMapper,
            @Value("${app.kafka.orders-topic}") String topic) {
        Map<String, Object> props = new HashMap<>(properties.buildProducerProperties(null));
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131_072);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33_554_432);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "orders-bulk");
        DefaultKafkaProducerFactory<String, Order> factory = new DefaultKafkaProducerFactory<>(props);
        factory.setKeySerializer(new StringSerializer());
        factory.setValueSerializer(new JsonSerializer<>(objectMapper));
        this.kafka = new KafkaTemplate<>(factory);
        this.topic = topic;
    }

    public BulkStatus start(long count) {
        if (count < 1 || count > MAX_COUNT) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "count must be between 1 and 10000000000");
        }
        Job job = new Job(count);
        while (true) {
            Job current = active.get();
            if (current != null && !current.finished()) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "A volume run is already in progress");
            }
            if (active.compareAndSet(current, job)) {
                break;
            }
        }
        executor.submit(() -> publish(job));
        return job.status();
    }

    public BulkStatus cancel() {
        Job job = active.get();
        if (job == null) {
            return null;
        }
        job.cancel();
        return job.status();
    }

    public BulkStatus current() {
        Job job = active.get();
        if (job == null) {
            return new BulkStatus(null, 0, 0, 0, "IDLE", 0, "");
        }
        return job.status();
    }

    private void publish(Job job) {
        Semaphore permits = new Semaphore(MAX_IN_FLIGHT);
        try {
            for (long i = 0; i < job.total && !job.cancelled(); i++) {
                permits.acquire();
                if (job.cancelled()) {
                    permits.release();
                    break;
                }
                String id = job.id + "-" + i;
                Order order = new Order(id, "bulk", "SKU-" + (i % 1_000), 1, Instant.now());
                try {
                    kafka.send(topic, id, order).whenComplete((result, error) -> {
                        if (error == null) {
                            job.sent.incrementAndGet();
                        } else {
                            job.failed.incrementAndGet();
                        }
                        permits.release();
                    });
                } catch (RuntimeException ex) {
                    job.failed.incrementAndGet();
                    permits.release();
                }
                if (i > 0 && i % 5_000 == 0 && job.sent.get() == 0 && job.failed.get() > 100) {
                    job.fail("Kafka is not accepting records");
                    break;
                }
            }
            permits.acquire(MAX_IN_FLIGHT);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            job.cancel();
        } finally {
            permits.release(MAX_IN_FLIGHT);
            job.finish();
        }
    }

    @PreDestroy
    public void close() {
        Job job = active.get();
        if (job != null) {
            job.cancel();
        }
        executor.shutdownNow();
        kafka.destroy();
    }

    public static final class Job {
        private final String id = UUID.randomUUID().toString();
        private final long total;
        private final Instant startedAt = Instant.now();
        private final AtomicLong sent = new AtomicLong();
        private final AtomicLong failed = new AtomicLong();
        private volatile String state = "RUNNING";
        private volatile String message = "";
        private volatile Instant finishedAt;

        private Job(long total) {
            this.total = total;
        }

        private boolean finished() {
            return !"RUNNING".equals(state);
        }

        private boolean cancelled() {
            return "CANCELLED".equals(state) || "FAILED".equals(state);
        }

        private void cancel() {
            if ("RUNNING".equals(state)) {
                state = "CANCELLED";
                message = "Stopped";
            }
        }

        private void fail(String reason) {
            if ("RUNNING".equals(state)) {
                state = "FAILED";
                message = reason;
            }
        }

        private void finish() {
            finishedAt = Instant.now();
            if ("RUNNING".equals(state)) {
                state = failed.get() > 0 && sent.get() == 0 ? "FAILED" : "DONE";
            }
        }

        public BulkStatus status() {
            Instant end = finishedAt == null ? Instant.now() : finishedAt;
            long elapsed = Math.max(1, end.toEpochMilli() - startedAt.toEpochMilli());
            long sentCount = sent.get();
            return new BulkStatus(id, total, sentCount, failed.get(), state, sentCount * 1000 / elapsed, message);
        }
    }

    public record BulkStatus(
            String id,
            long total,
            long sent,
            long failed,
            String state,
            long perSecond,
            String message) {
    }
}
