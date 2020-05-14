package com.sanket.kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MessageProducerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProducerService.class);

    public static void main(String[] args) {
        System.out.println("Hello world");

        //Create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create producer record
        String topic = "EVENTS";
        String content = "Hello from program  with callback!";
        String key = "HELLO1";
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, content);

        producer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                LOGGER.info(" Record published. Partition: {}, Offset:{}, Topic:{}, Timestamp: {}", recordMetadata.partition(),
                        recordMetadata.offset(), recordMetadata.topic(), recordMetadata.timestamp());
            } else {
                LOGGER.error("Error while publishing.", e);
            }
        });

        //flush it. Data won'tbe sent if we don'tdo flush or close
        producer.flush();
        producer.close();


    }
}
