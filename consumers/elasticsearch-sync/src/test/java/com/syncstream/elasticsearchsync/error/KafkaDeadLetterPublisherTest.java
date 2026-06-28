package com.syncstream.elasticsearchsync.error;

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
            "syncstream.errors.elasticsearch.products",
            mapper
        );

        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
            "syncstream.public.products",
            0,
            12L,
            "{\"id\":9}",
            "bad-json"
        );

        publisher.publish(record, new IllegalStateException("projection failure"));

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());

        ProducerRecord<String, String> sent = captor.getValue();
        assertEquals("syncstream.errors.elasticsearch.products", sent.topic());
        assertEquals("{\"id\":9}", sent.key());

        JsonNode payload = mapper.readTree(sent.value());
        assertEquals("syncstream.public.products", payload.get("sourceTopic").asText());
        assertEquals(0, payload.get("sourcePartition").asInt());
        assertEquals(12L, payload.get("sourceOffset").asLong());
        assertEquals("java.lang.IllegalStateException", payload.get("errorType").asText());
        assertTrue(payload.get("errorMessage").asText().contains("projection failure"));
    }
}
