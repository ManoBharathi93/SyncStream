package com.syncstream.redissync.config;

import java.util.Map;

public final class RedisSyncSettings {
    private final String kafkaBootstrapServers;
    private final String topic;
    private final String consumerGroupId;
    private final String autoOffsetReset;
    private final String redisHost;
    private final int redisPort;
    private final int maxRetryAttempts;
    private final long retryBackoffMs;
    private final String deadLetterTopic;

    public RedisSyncSettings(
        String kafkaBootstrapServers,
        String topic,
        String consumerGroupId,
        String autoOffsetReset,
        String redisHost,
        int redisPort,
        int maxRetryAttempts,
        long retryBackoffMs,
        String deadLetterTopic
    ) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.topic = topic;
        this.consumerGroupId = consumerGroupId;
        this.autoOffsetReset = autoOffsetReset;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.maxRetryAttempts = maxRetryAttempts;
        this.retryBackoffMs = retryBackoffMs;
        this.deadLetterTopic = deadLetterTopic;
    }

    public static RedisSyncSettings fromEnvironment() {
        Map<String, String> env = System.getenv();
        return new RedisSyncSettings(
            env.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            env.getOrDefault("KAFKA_TOPIC", "syncstream.public.products"),
            env.getOrDefault("KAFKA_GROUP_ID", "syncstream-redis-sync-v1"),
            env.getOrDefault("AUTO_OFFSET_RESET", "earliest"),
            env.getOrDefault("REDIS_HOST", "localhost"),
            Integer.parseInt(env.getOrDefault("REDIS_PORT", "6379")),
            Integer.parseInt(env.getOrDefault("MAX_RETRY_ATTEMPTS", "3")),
            Long.parseLong(env.getOrDefault("RETRY_BACKOFF_MS", "300")),
            env.getOrDefault("DEAD_LETTER_TOPIC", "syncstream.errors.redis.products")
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

    public String redisHost() {
        return redisHost;
    }

    public int redisPort() {
        return redisPort;
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
