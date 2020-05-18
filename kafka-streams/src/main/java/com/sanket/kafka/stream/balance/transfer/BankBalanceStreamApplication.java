package com.sanket.kafka.stream.balance.transfer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.util.Properties;

public class BankBalanceStreamApplication {

    private static final String topicBank = "bank-transactions";
    private static final String topicBankCount = "bank-transactions-count";

    public static void main(String[] args) {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        /* initializer */
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());
        try {
            KStream<Integer, JsonNode> transactionStream = streamsBuilder.stream(topicBank, Consumed.with(Serdes.Integer(), jsonSerde));

            ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
            initialBalance.put("count", 0);
            initialBalance.put("balance", 0);
            initialBalance.put("timestamp", Instant.ofEpochMilli(0L).toString());


            //we can also use 'reduce'
            KTable<Integer, JsonNode> transactionCount = transactionStream
                    .groupByKey(Grouped.with(Serdes.Integer(), jsonSerde)).aggregate(
                            () -> initialBalance,
                            (accountNo, transaction, balance) -> getNewBalance(transaction, balance),
                            Named.as("balance-count-aggregation"),
                            Materialized.with(Serdes.Integer(), jsonSerde)
                    );
            transactionCount.toStream().to(topicBankCount, Produced.with(Serdes.Integer(), jsonSerde));

            KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamProperties());
            System.out.println(kafkaStreams.toString());

            Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaStreams.close()));

            kafkaStreams.start();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static JsonNode getNewBalance(JsonNode transaction, JsonNode balance) {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("name", transaction.get("name").asText());
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());
        newBalance.put("timestamp", transaction.get("timestamp").asText());
        return newBalance;
    }

    private static Properties streamProperties() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-stream");
        //below serdes are not needed as we are directly putting the required serde in the stream code
//        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
//        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        return properties;
    }

}
