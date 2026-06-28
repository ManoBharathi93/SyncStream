package com.syncstream.redissync.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisSyncSettingsTest {

    @Test
    void constructorShouldExposeAllValues() {
        RedisSyncSettings settings = new RedisSyncSettings(
            "kafka:29092",
            "syncstream.public.products",
            "group-a",
            "latest",
            "redis-host",
            6380,
            5,
            900L,
            "syncstream.errors.redis.products"
        );

        assertEquals("kafka:29092", settings.kafkaBootstrapServers());
        assertEquals("syncstream.public.products", settings.topic());
        assertEquals("group-a", settings.consumerGroupId());
        assertEquals("latest", settings.autoOffsetReset());
        assertEquals("redis-host", settings.redisHost());
        assertEquals(6380, settings.redisPort());
        assertEquals(5, settings.maxRetryAttempts());
        assertEquals(900L, settings.retryBackoffMs());
        assertEquals("syncstream.errors.redis.products", settings.deadLetterTopic());
    }
}
