package com.sanket.kafka.stream.join.user.data;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;

public class UserDataPublisher {

    private static final String topicSourceDtls = "user-details";
    private static final String topicSourcePurchase = "user-details-purchase";

    public static void main(String[] args) {

        KafkaProducer<String, String> producer = kafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> producer.close()));

        try {
            producer.send(userRecord("john", "Email=john.doe@gmail.com")).get();
            producer.send(purchaseRecord("john", "Apples and Bananas (1)")).get();

            Thread.sleep(10000);

            // 2 - we receive user purchase, but it doesn't exist in Kafka
            System.out.println("\nExample 2 - non existing user\n");
            producer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get();

            Thread.sleep(10000);

            // 3 - we update user "john", and send a new transaction
            System.out.println("\nExample 3 - update to user\n");
            producer.send(userRecord("john", "Email=johnny.doe@gmail.com")).get();
            producer.send(purchaseRecord("john", "Oranges (3)")).get();

            Thread.sleep(10000);

            // 4 - we send a user purchase for stephane, but it exists in Kafka later
            System.out.println("\nExample 4 - non existing user then user\n");
            producer.send(purchaseRecord("stephane", "Computer (4)")).get();
            producer.send(userRecord("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph")).get();
            producer.send(purchaseRecord("stephane", "Books (4)")).get();
            producer.send(userRecord("stephane", null)).get(); // delete for cleanup

            Thread.sleep(10000);

            // 5 - we create a user, but it gets deleted before any purchase comes through
            System.out.println("\nExample 5 - user then delete then data\n");
            producer.send(userRecord("alice", "First=Alice")).get();
            producer.send(userRecord("alice", null)).get(); // that's the delete record
            producer.send(purchaseRecord("alice", "Apache Kafka Series (5)")).get();

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Published all the data!");

    }


    private static ProducerRecord<String, String> userRecord(String key, String value) {
        return new ProducerRecord<>(topicSourceDtls, key, value);
    }

    private static ProducerRecord<String, String> purchaseRecord(String key, String value) {
        return new ProducerRecord<>(topicSourcePurchase, key, value);
    }

    private static KafkaProducer<String, String> kafkaProducer() {
        return new KafkaProducer<>(kafkaProducerProperties());
    }

    private static Properties kafkaProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "5");

        return properties;
    }

}
