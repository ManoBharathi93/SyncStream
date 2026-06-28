package com.syncstream.elasticsearchsync.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.syncstream.elasticsearchsync.config.ElasticsearchSyncSettings;
import com.syncstream.elasticsearchsync.error.KafkaDeadLetterPublisher;
import com.syncstream.elasticsearchsync.model.DebeziumEnvelope;
import com.syncstream.elasticsearchsync.projection.ProductIndexProjector;
import com.syncstream.retry.BackoffStrategy;
import com.syncstream.retry.DefaultFailureClassifier;
import com.syncstream.retry.ExponentialBackoffStrategy;
import com.syncstream.retry.FailureClassifier;
import com.syncstream.retry.RetryPolicy;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticsearchSyncBootstrap {
    private static final Logger log = LoggerFactory.getLogger(ElasticsearchSyncBootstrap.class);

    public static void main(String[] args) {
        ElasticsearchSyncSettings settings = ElasticsearchSyncSettings.fromEnvironment();
        ObjectMapper mapper = new ObjectMapper();

        Properties props = new Properties();
        props.put("bootstrap.servers", settings.kafkaBootstrapServers());
        props.put("group.id", settings.consumerGroupId());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", settings.autoOffsetReset());
        props.put("enable.auto.commit", "false");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.kafkaBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
             KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
             RestClient esClient = RestClient.builder(new HttpHost(settings.elasticsearchHost(), settings.elasticsearchPort(), "http")).build()) {

            ProductIndexProjector projector = new ProductIndexProjector(esClient, mapper, settings.indexName());
            KafkaDeadLetterPublisher deadLetterPublisher = new KafkaDeadLetterPublisher(
                producer,
                settings.deadLetterTopic(),
                mapper
            );
            RetryPolicy retryPolicy = new RetryPolicy(
                settings.maxRetryAttempts(),
                settings.retryBackoffMs(),
                Math.max(settings.retryBackoffMs(), settings.retryBackoffMs() * 8L),
                0.20
            );
            FailureClassifier failureClassifier = new DefaultFailureClassifier();
            BackoffStrategy backoffStrategy = new ExponentialBackoffStrategy();
            consumer.subscribe(Collections.singletonList(settings.topic()));

            log.info("Elasticsearch Sync Consumer started. topic={}, groupId={}, offsetReset={}, index={}, es={}:{}, retries={}, backoffMs={}, deadLetterTopic={}",
                settings.topic(),
                settings.consumerGroupId(),
                settings.autoOffsetReset(),
                settings.indexName(),
                settings.elasticsearchHost(),
                settings.elasticsearchPort(),
                settings.maxRetryAttempts(),
                settings.retryBackoffMs(),
                settings.deadLetterTopic());

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        processWithRetry(record, mapper, projector, retryPolicy, failureClassifier, backoffStrategy);
                        consumer.commitSync();
                    } catch (Exception ex) {
                        log.error("Failed to process record topic={} partition={} offset={} key={}",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            ex);
                        deadLetterPublisher.publish(record, ex);
                        consumer.commitSync();
                    }
                }
            }
        } catch (Exception ex) {
            log.error("Elasticsearch sync consumer terminated with error", ex);
            throw new RuntimeException(ex);
        }
    }

    private static void processWithRetry(
        ConsumerRecord<String, String> record,
        ObjectMapper mapper,
        ProductIndexProjector projector,
        RetryPolicy retryPolicy,
        FailureClassifier failureClassifier,
        BackoffStrategy backoffStrategy
    ) throws Exception {
        if (record.value() == null) {
            return;
        }

        Exception last = null;
        for (int attempt = 1; attempt <= retryPolicy.maxAttempts(); attempt++) {
            try {
                DebeziumEnvelope event = parseEnvelope(mapper, record.value());
                projector.apply(record.topic(), record.key(), event);
                log.info("Processed record topic={} partition={} offset={} key={} op={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    event.op());
                return;
            } catch (Exception ex) {
                last = ex;
                if (!failureClassifier.shouldRetry(ex) || attempt >= retryPolicy.maxAttempts()) {
                    break;
                }

                long sleepMs = backoffStrategy.nextDelayMs(attempt, retryPolicy);
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ex;
                }
            }
        }

        if (last != null) {
            throw last;
        }
    }

    private static DebeziumEnvelope parseEnvelope(ObjectMapper mapper, String valueJson) throws Exception {
        JsonNode root = mapper.readTree(valueJson);
        JsonNode payload = root;
        if (root != null && root.isObject() && root.has("payload")) {
            payload = root.get("payload");
        }
        return mapper.treeToValue(payload, DebeziumEnvelope.class);
    }
}
