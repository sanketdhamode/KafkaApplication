package com.sanket.kafka.stream.adv;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class WordCountStreamApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-stream");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        String topic = "user.count.wordinput";
        String wordCountTopic = "user.count.wordoutput";

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //example to use serde is stream
        //KStream<String, String> inputStreams = streamsBuilder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> inputStreams = streamsBuilder.stream(topic);
        KTable<String, Long> transformedStream = inputStreams.mapValues(inputText -> inputText.toLowerCase())
                .peek((key, value) -> System.out.println(value))
                .flatMapValues(lowerCaseText -> Arrays.asList(lowerCaseText.split("\\s+")))
                .selectKey((ignoreKey, value) -> value)
                .groupByKey().count(Named.as("Counts"));

        transformedStream.toStream().to(wordCountTopic, Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaStreams.close()));
        kafkaStreams.start();
    }
}
