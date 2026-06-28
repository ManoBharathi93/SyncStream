package com.syncstream.redissync.error;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaDeadLetterPublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaDeadLetterPublisher.class);

    private final KafkaProducer<String, String> producer;
    private final String deadLetterTopic;
    private final ObjectMapper mapper;

    public KafkaDeadLetterPublisher(KafkaProducer<String, String> producer, String deadLetterTopic, ObjectMapper mapper) {
        this.producer = producer;
        this.deadLetterTopic = deadLetterTopic;
        this.mapper = mapper;
    }

    public void publish(ConsumerRecord<String, String> record, Exception ex) {
        try {
            ObjectNode payload = mapper.createObjectNode();
            payload.put("sourceTopic", record.topic());
            payload.put("sourcePartition", record.partition());
            payload.put("sourceOffset", record.offset());
            payload.put("key", record.key());
            payload.put("value", record.value());
            payload.put("errorType", ex.getClass().getName());
            payload.put("errorMessage", ex.getMessage());

            ProducerRecord<String, String> deadLetterRecord = new ProducerRecord<String, String>(
                deadLetterTopic,
                record.key(),
                payload.toString()
            );
            producer.send(deadLetterRecord).get();
            log.warn("Published record to dead-letter topic={} sourceTopic={} partition={} offset={}",
                deadLetterTopic, record.topic(), record.partition(), record.offset());
        } catch (Exception publishEx) {
            log.error("Failed to publish to dead-letter topic={} sourceTopic={} partition={} offset={}",
                deadLetterTopic, record.topic(), record.partition(), record.offset(), publishEx);
        }
    }
}
