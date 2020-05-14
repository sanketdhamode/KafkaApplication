package com.sanket.kafka.twitter;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class GoodProducer {

    private final static Logger LOGGER = LoggerFactory.getLogger(GoodProducer.class);
    private final static String consumerApiKey = "";
    private final static String consumerApiSecret = "";
    private final static String accessToken = "155574716-";
    private final static String accessTokenSecret = "";
    private final static String topic = "TWITTER_BULK_EVENTS";

    public static void main(String[] args) {

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties());

        LOGGER.info("Starting the twitter connection....");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>();
        List<String> terms = Lists.newArrayList("COVID-19");
        Client client = getTwitterClient(msgQueue, terms);

        try {
            client.connect();

            while (!client.isDone()) {
                String msg = msgQueue.poll(1000, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, msg);
                    kafkaProducer.send(producerRecord);
                    LOGGER.info(" Message with id {} published to kafka!", getId(msg));
                }

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
            kafkaProducer.close();
        }


    }


    private static String getId(String json) {
        JsonElement jsonElement = JsonParser.parseString(json);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        return jsonObject.get("id_str").getAsString();
    }

    private static Client getTwitterClient(BlockingQueue<String> msgQueue, List<String> terms) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);
        Authentication hosebirdAuth = new OAuth1(consumerApiKey, consumerApiSecret, accessToken, accessTokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));         // optional: use this if you want to process client events

        return builder.build();

    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //good producer configs
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");//ms

        return properties;
    }
}
