package com.sanket.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ManualCommitConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManualCommitConsumer.class);

    public static void main(String[] args) {
        Properties properties = getConsumerProperties();

        LOGGER.info("Connecting to the kafka....");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        try {
            kafkaConsumer.subscribe(Collections.singleton("EVENTS"));

            LOGGER.info("Connected to kafka server and subscribed to the topic!");

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                LOGGER.info("Total no of records fetched are: {}", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("Got message: Key:{},Value:{}, Partition:{}, Offset: {}", record.key(),
                            record.value(), record.partition(), record.offset());

                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                kafkaConsumer.commitAsync();
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        } finally {
            kafkaConsumer.close();
        }
    }

    private static Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "events-group");


        //manual commit settings
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "3");
        return properties;
    }
}
