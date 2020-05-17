package com.sanket.kafka.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaElasticSearchConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaElasticSearchConsumer.class);

    public static void main(String[] args) {

        String topic = "TWITTER_EVENTS";
        String esIndexName = "twitter";
        String esIndextype = "tweets";

        LOGGER.info("Connecting to elastic search cluster...");

        RestHighLevelClient highLevelClient = getElasticsearchClient();

        LOGGER.info("Connected to elastic search cluster!");

        KafkaConsumer<String, String> kafkaConsumer = getKafkaConsumer();
        kafkaConsumer.subscribe(Collections.singleton(topic));


        try {

            while (true) {

                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String tweetJson = record.value();
                    LOGGER.info(" Got the tweet data. Preparing to index it in elastic search!");
                    IndexRequest indexRequest
                            = new IndexRequest(esIndexName, esIndextype).source(tweetJson, XContentType.JSON);
                    IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);
                    LOGGER.info("Document indexed with index id: {} came fromm twitter", indexResponse.getId());

                    Thread.sleep(2000);
                }

            }

        } catch (IOException e) {
            LOGGER.error("Error while indexing the data!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                highLevelClient.close();
                kafkaConsumer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private static RestHighLevelClient getElasticsearchClient() {
        String host = "kafka-course-1932439202.ap-southeast-2.bonsaisearch.net";
        String username = "umhod0z9ox";
        String password = "knrg06vfh9";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(host, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        return new RestHighLevelClient(restClientBuilder);
    }


    private static KafkaConsumer<String, String> getKafkaConsumer() {
        return new KafkaConsumer<>(getKafkaConsumerPropeties());
    }

    private static Properties getKafkaConsumerPropeties() {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "twitter_group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }
}
