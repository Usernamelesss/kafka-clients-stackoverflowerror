package com.example.demo;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
@Profile("consume")
public class Consumer {
    private final ConcurrentMessageListenerContainer<String, String> container;

    public Consumer(ConcurrentMessageListenerContainer<String, String> container) {
        this.container = container;
    }

    @PostConstruct
    public void start() throws InterruptedException {
        this.container.start();
    }
}
