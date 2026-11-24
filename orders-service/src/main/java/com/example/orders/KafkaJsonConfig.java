package com.example.orders;

import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
public class KafkaJsonConfig {

    @Bean
    DefaultKafkaProducerFactoryCustomizer producerJson(ObjectMapper objectMapper) {
        return factory -> factory.setValueSerializer(new JsonSerializer<>(objectMapper));
    }

    @Bean
    DefaultKafkaConsumerFactoryCustomizer consumerJson(ObjectMapper objectMapper) {
        return factory -> factory.setValueDeserializer(new JsonDeserializer<>(objectMapper));
    }
}
