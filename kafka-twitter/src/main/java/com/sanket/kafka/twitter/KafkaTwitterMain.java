package com.sanket.kafka.twitter;

import com.google.common.collect.Lists;
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

public class KafkaTwitterMain {

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaTwitterMain.class);
    private final static String consumerApiKey = "vfNjS88ehfB7GlbdnaJ3Houdg";
    private final static String consumerApiSecret = "Itm4gz3yyBjMqAvTsc8GiPVTQIKvp6BLeYVzzo5DoYPx5Nvcve";
    private final static String accessToken = "155574716-L7Gu5cpeKp30cs3zlWRaMGj5aCYNFHZjGICiXR0i";
    private final static String accessTokenSecret = "yY5A79nsyOe8Hy7pTAUTHznfsUhgE0WRSXQIL8BanNoa9";


    public static void main(String[] args) {
        new KafkaTwitterMain().run();
    }

    private void run() {
        LOGGER.info("Starting the twitter connection....");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>();
        List<String> terms = Lists.newArrayList("Washim");
        Client client = getTwitterClient(msgQueue, terms);
        KafkaProducer<String, String> kafkaProducer = getKafkaProducer();
        String topic = "TWITTER_EVENTS";


        try {
            client.connect();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info(" Shutting down the  kafka and twitter client");
                kafkaProducer.close();
                client.stop();
            }));

            while (!client.isDone()) {
                try {
                    String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                    if (msg != null) {
                        LOGGER.info(msg);
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, msg);
                        kafkaProducer.send(producerRecord, (recordMetadata, e) -> LOGGER.info("Message published!"));
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("Got error.Closing the client");
                    client.stop();
                }

            }
        } finally {
            if (client != null)
                client.stop();
        }

        LOGGER.info("Stopping the twitter connection !");

    }

    private Client getTwitterClient(BlockingQueue<String> msgQueue, List<String> terms) {
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

    private KafkaProducer<String, String> getKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        return new KafkaProducer<>(properties);
    }
}
