package com.sanket.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MessageConsumerThreadMain {
    private static Logger LOGGER = LoggerFactory.getLogger(MessageConsumerThreadMain.class);

    public static void main(String[] args) {

        LOGGER.info("Starting the kafka consumer in thread mode!!!");

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "events-group-code");

        CountDownLatch countDownLatch = new CountDownLatch(1);
        String topic = "EVENTS";
        Runnable runnable = new MesssageConsumerThread(countDownLatch, topic, properties);
        Thread consumerThread = new Thread(runnable);
        consumerThread.start();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                ((MesssageConsumerThread) runnable).shutdown();
                countDownLatch.await();
            } catch (InterruptedException ex) {
                LOGGER.error("Application got interrupted");
            }
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error(" Application got interrupted.");
        } finally {
            LOGGER.info("Application closing!!!!");
        }
    }
}
