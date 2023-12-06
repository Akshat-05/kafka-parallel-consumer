package com.kafka.parallel.consumer.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${kafka.url}")
    private String kafkaUrl;

    @Value("${kafka.group}")
    private String groupId;

    @Value("${kafka.properties.ssl.endpoint.identification.algorithm}")
    private String sslAlgorithm;

    @Value("${kafka.properties.sasl.mechanism}")
    private String saslMechanism;

    @Value("${kafka.properties.sasl.jaas.config}")
    private String saslConfig;

    @Value("${kafka.properties.security.protocol}")
    private String securityProtocol;

    // Configure Kafka consumer properties
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Additional consumer properties
        props.put("ssl.endpoint.identification.algorithm", sslAlgorithm);
        props.put("sasl.mechanism", saslMechanism);
        props.put("sasl.jaas.config", saslConfig);
        props.put("security.protocol", securityProtocol);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // Configure the listener container factory for parallel consumption
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(6); // Set the number of consumer threads
        return factory;
    }
}
