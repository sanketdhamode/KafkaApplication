package com.sanket.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MesssageConsumerThread implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MesssageConsumerThread.class);
    private CountDownLatch countDownLatch;
    private String topic;
    private KafkaConsumer<String, String> kafkaConsumer;

    public MesssageConsumerThread(CountDownLatch latch, String topic, Properties consumerProperties) {
        this.countDownLatch = latch;
        this.topic = topic;
        this.kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    }

    @Override
    public void run() {
        try {
            kafkaConsumer.subscribe(Collections.singleton(topic));
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("Got message: Key:{},Value:{}, Partition:{}, Offset: {}", record.key(),
                            record.value(), record.partition(), record.offset());
                }
            }
        } catch (WakeupException ex) {
            LOGGER.error("Closing the consumer exception");
        } finally {
            countDownLatch.countDown();
        }

    }

    public void shutdown() {
        kafkaConsumer.wakeup();
        LOGGER.error("Shutdown called in thread");
    }
}
