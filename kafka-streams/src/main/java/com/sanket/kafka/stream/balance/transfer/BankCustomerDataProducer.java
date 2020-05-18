package com.sanket.kafka.stream.balance.transfer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class BankCustomerDataProducer {

    private static final String topicBank = "bank-transactions";

    public static void main(String[] args) {


        KafkaProducer<Integer, String> kafkaProducer = getKafkaConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaProducer.close();
        }));

        while (true) {
            try {
                kafkaProducer.send(producerRecord("sanket-1", 1111));
                Thread.sleep(5000);
                kafkaProducer.send(producerRecord("priyanka-1", 2222));
                Thread.sleep(5000);
                kafkaProducer.send(producerRecord("ajay-1", 3333));
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
            }
        }


    }

    private static ProducerRecord<Integer, String> producerRecord(String name, Integer accountNo) {
        ObjectNode jsonNodes = JsonNodeFactory.instance.objectNode();
        Integer randomAmt = new Random().nextInt(100);
        jsonNodes.put("accountNo", accountNo);
        jsonNodes.put("name", name);
        jsonNodes.put("amount", 100);
        jsonNodes.put("timestamp", Instant.now().toString());
        return new ProducerRecord<>(topicBank, accountNo, jsonNodes.toString());

    }

    private static KafkaProducer<Integer, String> getKafkaConsumer() {
        return new KafkaProducer<>(kafkaProperties());
    }

    private static Properties kafkaProperties() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //idempotent
        kafkaProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        kafkaProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        //high throughput
        kafkaProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        kafkaProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        kafkaProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        kafkaProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        kafkaProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return kafkaProperties;
    }
}
