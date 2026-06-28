package com.syncstream.redissync.error;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaDeadLetterPublisherTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void shouldPublishStructuredDeadLetterPayload() throws Exception {
        KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(null));

        KafkaDeadLetterPublisher publisher = new KafkaDeadLetterPublisher(
            producer,
            "syncstream.errors.redis.products",
            mapper
        );

        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
            "syncstream.public.products",
            2,
            91L,
            "{\"id\":10}",
            "bad-json"
        );

        publisher.publish(record, new IllegalArgumentException("boom"));

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());

        ProducerRecord<String, String> sent = captor.getValue();
        assertEquals("syncstream.errors.redis.products", sent.topic());
        assertEquals("{\"id\":10}", sent.key());

        JsonNode payload = mapper.readTree(sent.value());
        assertEquals("syncstream.public.products", payload.get("sourceTopic").asText());
        assertEquals(2, payload.get("sourcePartition").asInt());
        assertEquals(91L, payload.get("sourceOffset").asLong());
        assertEquals("java.lang.IllegalArgumentException", payload.get("errorType").asText());
        assertTrue(payload.get("errorMessage").asText().contains("boom"));
    }
}
