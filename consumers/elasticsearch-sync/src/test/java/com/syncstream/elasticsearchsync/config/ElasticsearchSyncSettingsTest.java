package com.syncstream.elasticsearchsync.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ElasticsearchSyncSettingsTest {

    @Test
    void constructorShouldExposeAllValues() {
        ElasticsearchSyncSettings settings = new ElasticsearchSyncSettings(
            "kafka:29092",
            "syncstream.public.products",
            "group-1",
            "latest",
            "localhost",
            9200,
            "products_v1",
            4,
            500L,
            "syncstream.errors.elasticsearch.products"
        );

        assertEquals("kafka:29092", settings.kafkaBootstrapServers());
        assertEquals("syncstream.public.products", settings.topic());
        assertEquals("group-1", settings.consumerGroupId());
        assertEquals("latest", settings.autoOffsetReset());
        assertEquals("localhost", settings.elasticsearchHost());
        assertEquals(9200, settings.elasticsearchPort());
        assertEquals("products_v1", settings.indexName());
        assertEquals(4, settings.maxRetryAttempts());
        assertEquals(500L, settings.retryBackoffMs());
        assertEquals("syncstream.errors.elasticsearch.products", settings.deadLetterTopic());
    }
}
