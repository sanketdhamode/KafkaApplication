package com.sanket.kafka.stream.adv;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class ColorCountStreamApplication {

    public static void main(String[] args) {

        Properties streamProperties = new Properties();
        streamProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "color-count-application");
        streamProperties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        streamProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


        String topicInput = "user.color.input";
        String topicInputIntermidiatory = "user.color.count.input.inter";
        String topicOutput = "user.color.count.output";

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputStream = streamsBuilder.stream(topicInput);
        KStream<String, String> newStream = inputStream
                .filter((key, value) -> value != null && value.contains(",") && value.split(",").length == 2)
                .mapValues(values -> values.toLowerCase())
                .selectKey((key, value) -> value.split(",")[0])
                .mapValues(value -> value.split(",")[1]);
        newStream.to(topicInputIntermidiatory);

        KTable<String, String> inputStreamTable = streamsBuilder.table(topicInputIntermidiatory);
        KTable<String, Long> stringLongKTable = inputStreamTable
                .groupBy((user, color) -> new KeyValue<>(color, color)).count(Named.as("CountsByColors"));

        stringLongKTable.toStream().to(topicOutput, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamProperties);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
        }));

    }
}
