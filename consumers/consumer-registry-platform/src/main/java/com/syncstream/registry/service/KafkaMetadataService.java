package com.syncstream.registry.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Collection;
import java.util.TreeMap;

public class KafkaMetadataService {
    private final String bootstrapServers;

    public KafkaMetadataService(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public List<Map<String, Object>> topics() {
        try (AdminClient adminClient = AdminClient.create(adminConfig())) {
            ListTopicsResult result = adminClient.listTopics();
            Collection<TopicListing> listings = result.listings().get();
            List<Map<String, Object>> topics = new ArrayList<Map<String, Object>>();
            for (TopicListing listing : listings) {
                Map<String, Object> topic = new LinkedHashMap<String, Object>();
                topic.put("topic", listing.name());
                topic.put("internal", listing.isInternal());
                topic.put("partitions", partitionCount(adminClient, listing.name()));
                topic.put("consumerLag", consumerLagForTopic(listing.name()));
                topic.put("oldestOffset", oldestOffset(listing.name()));
                topic.put("latestOffset", latestOffset(listing.name()));
                topics.add(topic);
            }
            return topics;
        } catch (Exception ex) {
            return Collections.<Map<String, Object>>emptyList();
        }
    }

    public Map<String, Object> health() {
        Map<String, Object> health = new LinkedHashMap<String, Object>();
        try (AdminClient adminClient = AdminClient.create(adminConfig())) {
            health.put("status", "healthy");
            health.put("brokerCount", adminClient.describeCluster().nodes().get().size());
            health.put("topics", topics().size());
            health.put("consumerLag", aggregateConsumerLag());
        } catch (Exception ex) {
            health.put("status", "degraded");
            health.put("error", ex.getMessage());
        }
        return health;
    }

    public List<Map<String, Object>> consumerHealth() {
        try (AdminClient adminClient = AdminClient.create(adminConfig())) {
            Map<String, Map<TopicPartition, Long>> offsets = consumerGroupOffsets(adminClient);
            List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
            for (Map.Entry<String, Map<TopicPartition, Long>> entry : offsets.entrySet()) {
                for (Map.Entry<TopicPartition, Long> partitionOffset : entry.getValue().entrySet()) {
                    Map<String, Object> row = new LinkedHashMap<String, Object>();
                    String topic = partitionOffset.getKey().topic();
                    row.put("consumer", entry.getKey());
                    row.put("topic", topic);
                    row.put("partition", partitionOffset.getKey().partition());
                    row.put("lag", Math.max(0, latestOffset(topic, partitionOffset.getKey().partition()) - partitionOffset.getValue()));
                    row.put("status", "healthy");
                    rows.add(row);
                }
            }
            return rows;
        } catch (Exception ex) {
            return Collections.<Map<String, Object>>emptyList();
        }
    }

    public List<Map<String, Object>> dlqEvents(String topicFilter, String limit) {
        int max = parseLimit(limit, 25);
        List<String> topics = new ArrayList<String>();
        try (AdminClient adminClient = AdminClient.create(adminConfig())) {
            for (TopicListing listing : adminClient.listTopics().listings().get()) {
                if (listing.name().startsWith("syncstream.errors.")) {
                    topics.add(listing.name());
                }
            }
        } catch (Exception ex) {
            return Collections.<Map<String, Object>>emptyList();
        }

        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        for (String topic : topics) {
            if (topicFilter != null && !topicFilter.trim().isEmpty() && !topic.contains(topicFilter.trim())) {
                continue;
            }
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfig("dashboard-dlq-reader"))) {
                consumer.subscribe(Collections.singletonList(topic));
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1500));
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> row = new LinkedHashMap<String, Object>();
                    row.put("topic", record.topic());
                    row.put("partition", record.partition());
                    row.put("offset", record.offset());
                    row.put("key", record.key());
                    row.put("value", record.value());
                    rows.add(row);
                    if (rows.size() >= max) {
                        return rows;
                    }
                }
            } catch (Exception ex) {
                return Collections.<Map<String, Object>>emptyList();
            }
        }
        return rows;
    }

    public Map<String, Object> replayRequestSummary(String topic, String consumerGroup, String startTimestamp, String startOffset, String endOffset, String reason, String actor) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("topic", topic);
        payload.put("consumerGroup", consumerGroup);
        payload.put("startTimestamp", startTimestamp);
        payload.put("startOffset", startOffset);
        payload.put("endOffset", endOffset);
        payload.put("reason", reason);
        payload.put("actor", actor);
        return payload;
    }

    private Properties adminConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("request.timeout.ms", "1000");
        props.put("default.api.timeout.ms", "1000");
        props.put("connections.max.idle.ms", "1000");
        props.put("reconnect.backoff.max.ms", "50");
        props.put("retries", "0");
        return props;
    }

    private Properties consumerConfig(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "25");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "1000");
        props.put("connections.max.idle.ms", "1000");
        props.put("reconnect.backoff.max.ms", "50");
        props.put("retries", "0");
        return props;
    }

    private int partitionCount(AdminClient adminClient, String topic) throws Exception {
        return adminClient.describeTopics(Collections.singletonList(topic)).all().get().get(topic).partitions().size();
    }

    private long oldestOffset(String topic) throws Exception {
        return offsetForTopic(topic, true);
    }

    private long latestOffset(String topic) throws Exception {
        return offsetForTopic(topic, false);
    }

    private long latestOffset(String topic, int partition) throws Exception {
        try (AdminClient adminClient = AdminClient.create(adminConfig())) {
            Map<TopicPartition, Long> offsets = consumerGroupOffsets(adminClient).getOrDefault(topic, Collections.<TopicPartition, Long>emptyMap());
            return offsets.containsKey(new TopicPartition(topic, partition)) ? offsets.get(new TopicPartition(topic, partition)) : 0L;
        }
    }

    private long offsetForTopic(String topic, boolean oldest) throws Exception {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfig("offset-reader-" + topic + (oldest ? "-oldest" : "-latest")))) {
            List<TopicPartition> partitions = new ArrayList<TopicPartition>();
            consumer.partitionsFor(topic).forEach(info -> partitions.add(new TopicPartition(topic, info.partition())));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long total = 0L;
            for (TopicPartition partition : partitions) {
                if (oldest) {
                    total += consumer.position(partition);
                } else {
                    consumer.seekToEnd(Collections.singletonList(partition));
                    total += consumer.position(partition);
                }
            }
            return total;
        } catch (Exception ex) {
            return 0L;
        }
    }

    private long consumerLagForTopic(String topic) {
        try (AdminClient adminClient = AdminClient.create(adminConfig())) {
            long total = 0L;
            Map<String, Map<TopicPartition, Long>> offsets = consumerGroupOffsets(adminClient);
            for (Map.Entry<String, Map<TopicPartition, Long>> entry : offsets.entrySet()) {
                for (Map.Entry<TopicPartition, Long> partitionOffset : entry.getValue().entrySet()) {
                    if (!partitionOffset.getKey().topic().equals(topic)) {
                        continue;
                    }
                    long latest = latestOffset(topic, partitionOffset.getKey().partition());
                    total += Math.max(0, latest - partitionOffset.getValue());
                }
            }
            return total;
        } catch (Exception ex) {
            return 0L;
        }
    }

    private long aggregateConsumerLag() {
        try (AdminClient adminClient = AdminClient.create(adminConfig())) {
            long total = 0L;
            Map<String, Map<TopicPartition, Long>> offsets = consumerGroupOffsets(adminClient);
            for (Map.Entry<String, Map<TopicPartition, Long>> entry : offsets.entrySet()) {
                for (Map.Entry<TopicPartition, Long> partitionOffset : entry.getValue().entrySet()) {
                    String topic = partitionOffset.getKey().topic();
                    long latest = latestOffset(topic, partitionOffset.getKey().partition());
                    total += Math.max(0, latest - partitionOffset.getValue());
                }
            }
            return total;
        } catch (Exception ex) {
            return 0L;
        }
    }

    private Map<String, Map<TopicPartition, Long>> consumerGroupOffsets(AdminClient adminClient) throws Exception {
        Map<String, Map<TopicPartition, Long>> groups = new TreeMap<String, Map<TopicPartition, Long>>();
        for (String groupId : adminClient.listConsumerGroups().all().get().stream().map(g -> g.groupId()).toArray(String[]::new)) {
            ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, Long> offsets = result.partitionsToOffsetAndMetadata().get().entrySet().stream().collect(
                java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset())
            );
            groups.put(groupId, offsets);
        }
        return groups;
    }

    private int parseLimit(String raw, int defaultValue) {
        try {
            if (raw == null || raw.trim().isEmpty()) {
                return defaultValue;
            }
            return Math.max(1, Math.min(100, Integer.parseInt(raw.trim())));
        } catch (Exception ex) {
            return defaultValue;
        }
    }
}
