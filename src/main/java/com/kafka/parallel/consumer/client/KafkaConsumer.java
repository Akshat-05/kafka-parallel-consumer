package com.kafka.parallel.consumer.client;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(topics = "evt_notification", groupId = "test")
    public void consume(String message) {
        // Process the Kafka message
        //System.out.println("thread id {}"+ Thread.currentThread().getId() + "Received message: " + message);
        System.out.println("Received message: " + message);
    }
}
