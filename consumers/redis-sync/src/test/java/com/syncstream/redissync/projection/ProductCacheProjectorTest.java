package com.syncstream.redissync.projection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncstream.redissync.model.DebeziumEnvelope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPooled;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class ProductCacheProjectorTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private JedisPooled redis;
    private ProductCacheProjector projector;

    @BeforeEach
    void setUp() {
        redis = mock(JedisPooled.class);
        projector = new ProductCacheProjector(redis, mapper);
    }

    @Test
    void shouldIgnoreNonProductTopics() throws Exception {
        DebeziumEnvelope event = new DebeziumEnvelope();
        event.setOp("u");
        event.setAfter(mapper.readTree("{\"id\":1,\"name\":\"A\"}"));

        projector.apply("syncstream.public.orders", "{\"id\":1}", event);

        verifyNoInteractions(redis);
    }

    @Test
    void shouldUpsertOnCreateOrUpdateWithWrappedKeyPayload() throws Exception {
        DebeziumEnvelope event = new DebeziumEnvelope();
        event.setOp("u");
        event.setAfter(mapper.readTree("{\"id\":123,\"name\":\"Test\"}"));

        projector.apply(
            "syncstream.public.products",
            "{\"schema\":{},\"payload\":{\"id\":123}}",
            event
        );

        verify(redis).set("cache:products:123", "{\"id\":123,\"name\":\"Test\"}");
    }

    @Test
    void shouldSupportPrimitiveKeyPayload() throws Exception {
        DebeziumEnvelope event = new DebeziumEnvelope();
        event.setOp("u");
        event.setAfter(mapper.readTree("{\"id\":77,\"name\":\"Primitive\"}"));

        projector.apply("syncstream.public.products", "77", event);

        verify(redis).set("cache:products:77", "{\"id\":77,\"name\":\"Primitive\"}");
    }

    @Test
    void shouldDeleteOnDeleteEvent() {
        DebeziumEnvelope event = new DebeziumEnvelope();
        event.setOp("d");

        projector.apply("syncstream.public.products", "{\"id\":66}", event);

        verify(redis).del("cache:products:66");
        verify(redis, never()).set(anyString(), anyString());
    }

    @Test
    void shouldSkipWhenAfterIsNullForNonDelete() {
        DebeziumEnvelope event = new DebeziumEnvelope();
        event.setOp("u");

        projector.apply("syncstream.public.products", "{\"id\":45}", event);

        verify(redis, never()).set(anyString(), anyString());
        verify(redis, never()).del(anyString());
    }
}
