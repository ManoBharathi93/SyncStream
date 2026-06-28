package com.syncstream.registry.model;

import java.util.List;
import java.util.Map;

public final class DashboardModels {
    private DashboardModels() {
    }

    public static class HealthResponse {
        public String status;
        public String generatedAt;
        public Map<String, Object> cdc;
        public Map<String, Object> kafka;
        public Map<String, Object> registry;
        public Map<String, Object> dlq;
        public Map<String, Object> metrics;
    }

    public static class MetricsResponse {
        public Map<String, Object> registry;
        public Map<String, Object> prometheus;
        public Map<String, Object> processing;
    }

    public static class TopicSummary {
        public String topic;
        public int partitions;
        public long oldestOffset;
        public long latestOffset;
        public long lag;
        public long messagesPerSecond;
    }

    public static class ConsumerHealthSummary {
        public String consumer;
        public String topic;
        public String environment;
        public String status;
        public long lag;
        public String ownerTeam;
        public String updatedAt;
    }

    public static class DlqEventSummary {
        public String topic;
        public String partition;
        public String offset;
        public String key;
        public String errorType;
        public String errorMessage;
        public String createdAt;
    }

    public static class ReplayRequestSummary {
        public String id;
        public String topic;
        public String consumerGroup;
        public String startTimestamp;
        public String status;
        public String reason;
        public String createdAt;
        public String createdBy;
    }

    public static class DashboardLandingResponse {
        public HealthResponse health;
        public List<TopicSummary> topics;
        public List<ConsumerHealthSummary> consumers;
        public List<DlqEventSummary> dlq;
        public List<ReplayRequestSummary> replayRequests;
        public Map<String, Object> metrics;
    }
}
