package com.sanket.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerSeekNAssign {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerSeekNAssign.class);


    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        String topic = "EVENTS";

        //assign the topic partition
        TopicPartition topicPartition = new TopicPartition(topic, 2);
        kafkaConsumer.assign(Collections.singleton(topicPartition));

        long fromOffset = 10;
        kafkaConsumer.seek(topicPartition, fromOffset);
        boolean continueLoop = true;
        int messageConsumedCnt = 0;

        while (continueLoop) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("Got message: Key:{},Value:{}, Partition:{}, Offset: {}", record.key(),
                        record.value(), record.partition(), record.offset());
                messageConsumedCnt++;
                if (messageConsumedCnt >= 5){
                    continueLoop = false;
                    break;
                }
            }



        }

    }
}
