package com.sanket.kafka.stream.join.user.data;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class UserDataJoinStreamApplication {

    private static final String topicSourceDtls = "user-details";
    private static final String topicSourcePurchase = "user-details-purchase";

    private static final String topicInterInnerJoin = "user-details-purchase-inner";
    private static final String topicInterLeftJoin = "user-details-purchase-left";

    public static void main(String[] args) {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        GlobalKTable<String, String> globalKTableUserDtls = streamsBuilder.globalTable(topicSourceDtls);
        KStream<String, String> kStreamPurchase = streamsBuilder.stream(topicSourcePurchase);

        KStream<String, String> innerJoinStream = kStreamPurchase.join(globalKTableUserDtls,
                (key, value) -> key,
                (userPurcase, userDtls) -> "User Details: " + userDtls + " Purchase Details: " + userPurcase
        );

        innerJoinStream.to(topicInterInnerJoin);

        KStream<String, String> leftJoinStream = kStreamPurchase.leftJoin(globalKTableUserDtls,
                (key, value) -> key,
                (userPurchase, userDtls) -> {
                    if (userDtls != null) {
                        return "User Details: " + userDtls + " Purchase Details: " + userPurchase;
                    } else {
                        return " Purchase Details: " + userPurchase;
                    }
                }
        );
        leftJoinStream.to(topicInterLeftJoin);

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kafkaStreamProperties());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaStreams.close()));
        kafkaStreams.start();


    }

    private static Properties kafkaStreamProperties() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "user-data-join-stream");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        return properties;
    }
}
