package com.sanket.kafka.stream;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaTweetStreamFilter {

    private final static String topic = "TWITTER_BULK_EVENTS";
    private final static String topicFiltered = "TWITTER_FILTER_EVENTS";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTweetStreamFilter.class);

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "stream-application-id");


        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputStream = streamsBuilder.stream(topic);
        KStream<String, String> filteredStream = inputStream.filter((k, v) -> getFollowersCount(v) > 10000);
        filteredStream.to(topicFiltered);
        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        //start out stream application
        LOGGER.info("Starting the kafka stream application!");
        kafkaStreams.start();
    }

    private static int getFollowersCount(String json) {
        try {
            JsonElement jsonElement = JsonParser.parseString(json);
            return jsonElement.getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();

        } catch (NullPointerException ex) {
            return 0;
        }
    }
}
