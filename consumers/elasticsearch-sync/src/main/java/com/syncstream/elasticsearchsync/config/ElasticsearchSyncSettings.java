package com.syncstream.elasticsearchsync.config;

import java.util.Map;

public final class ElasticsearchSyncSettings {
    private final String kafkaBootstrapServers;
    private final String topic;
    private final String consumerGroupId;
    private final String autoOffsetReset;
    private final String elasticsearchHost;
    private final int elasticsearchPort;
    private final String indexName;
    private final int maxRetryAttempts;
    private final long retryBackoffMs;
    private final String deadLetterTopic;

    public ElasticsearchSyncSettings(
        String kafkaBootstrapServers,
        String topic,
        String consumerGroupId,
        String autoOffsetReset,
        String elasticsearchHost,
        int elasticsearchPort,
        String indexName,
        int maxRetryAttempts,
        long retryBackoffMs,
        String deadLetterTopic
    ) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.topic = topic;
        this.consumerGroupId = consumerGroupId;
        this.autoOffsetReset = autoOffsetReset;
        this.elasticsearchHost = elasticsearchHost;
        this.elasticsearchPort = elasticsearchPort;
        this.indexName = indexName;
        this.maxRetryAttempts = maxRetryAttempts;
        this.retryBackoffMs = retryBackoffMs;
        this.deadLetterTopic = deadLetterTopic;
    }

    public static ElasticsearchSyncSettings fromEnvironment() {
        Map<String, String> env = System.getenv();
        return new ElasticsearchSyncSettings(
            env.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            env.getOrDefault("KAFKA_TOPIC", "syncstream.public.products"),
            env.getOrDefault("KAFKA_GROUP_ID", "syncstream-elasticsearch-sync-v1"),
            env.getOrDefault("AUTO_OFFSET_RESET", "earliest"),
            env.getOrDefault("ELASTICSEARCH_HOST", "localhost"),
            Integer.parseInt(env.getOrDefault("ELASTICSEARCH_PORT", "9200")),
            env.getOrDefault("ELASTICSEARCH_INDEX", "products_v1"),
            Integer.parseInt(env.getOrDefault("MAX_RETRY_ATTEMPTS", "3")),
            Long.parseLong(env.getOrDefault("RETRY_BACKOFF_MS", "300")),
            env.getOrDefault("DEAD_LETTER_TOPIC", "syncstream.errors.elasticsearch.products")
        );
    }

    public String kafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String topic() {
        return topic;
    }

    public String consumerGroupId() {
        return consumerGroupId;
    }

    public String autoOffsetReset() {
        return autoOffsetReset;
    }

    public String elasticsearchHost() {
        return elasticsearchHost;
    }

    public int elasticsearchPort() {
        return elasticsearchPort;
    }

    public String indexName() {
        return indexName;
    }

    public int maxRetryAttempts() {
        return maxRetryAttempts;
    }

    public long retryBackoffMs() {
        return retryBackoffMs;
    }

    public String deadLetterTopic() {
        return deadLetterTopic;
    }
}
