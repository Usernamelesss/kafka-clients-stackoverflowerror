package com.example.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;


@Service
@Profile("produce")
public class Producer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    private final KafkaTemplate<String, String> producer;
    private final AdminClient kafkaAdmin;

    @Value(value = "${spring.kafka.template.default-topic}")
    private String topic;

    public Producer(KafkaTemplate<String, String> producer, AdminClient kafkaAdmin, KafkaAdmin admin) {
        this.producer = producer;
        this.kafkaAdmin = kafkaAdmin;
        admin.initialize();
    }

    @PostConstruct
    public void produceRecord() {
        LOGGER.info("Waiting for topic init");
        waitForTopic();
        int recordsToBeProduced = 500000;
        LOGGER.info("Topic is ready, start producing {} messages", recordsToBeProduced);
        for (int i = 0; i <= recordsToBeProduced; i++) {
            try {
                producer.send(topic, String.valueOf(i)).get();

            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Error producing", e);
                System.exit(1);
            }
        }
        LOGGER.info("Produced {} records", recordsToBeProduced);
    }

    private void waitForTopic() {
        boolean ready = false;
        while (!ready) {
            try {
                Thread.sleep(10000);
                DescribeTopicsResult res = kafkaAdmin.describeTopics(List.of(topic));
                ready = res.values().get(topic).get().partitions().size() == 100;
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Error on waitForTopic", e);
            }
        }
    }
}
