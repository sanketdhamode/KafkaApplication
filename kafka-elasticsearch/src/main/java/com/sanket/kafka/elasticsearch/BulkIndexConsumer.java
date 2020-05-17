package com.sanket.kafka.elasticsearch;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
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

public class BulkIndexConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkIndexConsumer.class);
    private final static String topic = "TWITTER_BULK_EVENTS";

    public static void main(String[] args) {

        RestHighLevelClient restHighLevelClient = getElasticsearchClient();

        KafkaConsumer<String, String> kafkaConsumer = getKafkaConsumer();

        kafkaConsumer.subscribe(Collections.singleton(topic));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            kafkaConsumer.close();

            try {
                restHighLevelClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        String index = "twitter_covid";

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

            LOGGER.info("Fetched {} records from kafka", records.count());

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                String jsonData = record.value();
                String docId = getId(jsonData);
                IndexRequest indexRequest = new IndexRequest(index).id(docId)
                        .source(jsonData, XContentType.JSON);
                bulkRequest.add(indexRequest);
            }

            try {
                if (records.count() > 0) {
                    restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    LOGGER.info("{} records written to elastic search", records.count());
                    kafkaConsumer.commitAsync();
                    LOGGER.info("Offset has been committed");
                }

                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private static String getId(String jsonData) {
        JsonElement jsonElement = JsonParser.parseString(jsonData);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        return jsonObject.get("id_str").getAsString();
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
        return new KafkaConsumer<>(getKafkaConsumerProperties());
    }

    private static Properties getKafkaConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "twitter_covid_group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
        return properties;
    }
}
