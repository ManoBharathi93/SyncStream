package com.syncstream.registry.service;

import com.syncstream.registry.api.ReplayRequest;
import com.syncstream.registry.db.DatabaseManager;
import com.syncstream.registry.db.ManagementAuditRepository;
import com.syncstream.registry.db.ReplayRepository;
import com.syncstream.registry.model.ConsumerRegistration;
import com.syncstream.registry.model.DashboardModels;
import com.syncstream.registry.model.ReplayRequestRecord;
import com.syncstream.registry.util.Jsons;
import com.syncstream.registry.util.Times;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ManagementService {
    private final DatabaseManager databaseManager;
    private final RegistryService registryService;
    private final RouteQueryService routeQueryService;
    private final KafkaMetadataService kafkaMetadataService;
    private final PrometheusService prometheusService;
    private final ReplayRepository replayRepository;
    private final ManagementAuditRepository managementAuditRepository;
    private final PlatformMetrics platformMetrics;

    public ManagementService(
        DatabaseManager databaseManager,
        RegistryService registryService,
        RouteQueryService routeQueryService,
        KafkaMetadataService kafkaMetadataService,
        PrometheusService prometheusService,
        ReplayRepository replayRepository,
        ManagementAuditRepository managementAuditRepository,
        PlatformMetrics platformMetrics
    ) {
        this.databaseManager = databaseManager;
        this.registryService = registryService;
        this.routeQueryService = routeQueryService;
        this.kafkaMetadataService = kafkaMetadataService;
        this.prometheusService = prometheusService;
        this.replayRepository = replayRepository;
        this.managementAuditRepository = managementAuditRepository;
        this.platformMetrics = platformMetrics;
    }

    public DashboardModels.HealthResponse health() {
        DashboardModels.HealthResponse response = new DashboardModels.HealthResponse();
        response.generatedAt = Times.nowIsoUtc();
        Map<String, Object> kafkaHealth = kafkaMetadataService.health();
        response.cdc = kafkaHealth;
        response.kafka = kafkaHealth;
        response.registry = registryHealth();
        response.dlq = dlqHealth();
        response.metrics = platformMetrics.snapshot();
        response.status = "healthy";
        return response;
    }

    public DashboardModels.MetricsResponse metrics() {
        DashboardModels.MetricsResponse response = new DashboardModels.MetricsResponse();
        response.registry = registryService.metrics();
        try {
            response.prometheus = prometheusService.query("sum(kafka_topic_partition_current_offset)");
        } catch (Exception ex) {
            Map<String, Object> fallback = new HashMap<String, Object>();
            fallback.put("status", "unavailable");
            fallback.put("message", ex.getMessage() == null ? "prometheus query failed" : ex.getMessage());
            response.prometheus = fallback;
        }
        response.processing = platformMetrics.snapshot();
        return response;
    }

    public List<Map<String, Object>> topics() {
        return kafkaMetadataService.topics();
    }

    public List<Map<String, Object>> consumers() {
        try {
            List<ConsumerRegistration> registrations = registryService.list(new HashMap<String, String>());
            List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
            for (ConsumerRegistration registration : registrations) {
                Map<String, Object> row = new HashMap<String, Object>();
                row.put("id", registration.id());
                row.put("consumer", registration.consumer());
                row.put("topic", registration.topic());
                row.put("environment", registration.environment());
                row.put("ownerTeam", registration.ownerTeam());
                row.put("status", registration.status().toDbValue());
                row.put("version", registration.version());
                row.put("updatedAt", registration.updatedAt());
                rows.add(row);
            }
            return rows;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to list consumers", ex);
        }
    }

    public List<Map<String, Object>> dlq(String topicFilter, String limit) {
        return kafkaMetadataService.dlqEvents(topicFilter, limit);
    }

    public ReplayRequestRecord requestReplay(ReplayRequest request) {
        String actor = request.actor == null || request.actor.trim().isEmpty() ? "system" : request.actor.trim();
        String now = Times.nowIsoUtc();
        ReplayRequestRecord record = new ReplayRequestRecord(
            UUID.randomUUID().toString(),
            request.topic,
            request.consumerGroup,
            request.startTimestamp,
            request.startOffset,
            request.endOffset,
            request.reason,
            "QUEUED",
            now,
            actor,
            now,
            actor
        );
        try (Connection connection = databaseManager.connection()) {
            connection.setAutoCommit(false);
            replayRepository.insert(connection, record);
            managementAuditRepository.insert(connection, new com.syncstream.registry.model.ManagementAuditRecord(
                UUID.randomUUID().toString(),
                "REPLAY_REQUEST",
                "replay_request",
                record.id(),
                null,
                Jsons.toJson(record),
                actor,
                now
            ));
            connection.commit();
            return record;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to queue replay request", ex);
        }
    }

    public List<com.syncstream.registry.model.ManagementAuditRecord> activity() {
        try (Connection connection = databaseManager.connection()) {
            return managementAuditRepository.list(connection);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to list activity", ex);
        }
    }

    public List<ReplayRequestRecord> replayRequests() {
        try (Connection connection = databaseManager.connection()) {
            return replayRepository.list(connection);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to list replay requests", ex);
        }
    }

    public DashboardModels.DashboardLandingResponse dashboardLanding() {
        DashboardModels.DashboardLandingResponse landing = new DashboardModels.DashboardLandingResponse();
        try { landing.health = health(); } catch (Exception ignored) { landing.health = degradedKafkaHealth(); }
        try { landing.topics = convertTopics(topics()); } catch (Exception ignored) { landing.topics = new ArrayList<DashboardModels.TopicSummary>(); }
        try { landing.consumers = convertConsumers(consumers()); } catch (Exception ignored) { landing.consumers = new ArrayList<DashboardModels.ConsumerHealthSummary>(); }
        try { landing.dlq = convertDlq(dlq(null, "10")); } catch (Exception ignored) { landing.dlq = new ArrayList<DashboardModels.DlqEventSummary>(); }
        try { landing.replayRequests = convertReplay(replayRequests()); } catch (Exception ignored) { landing.replayRequests = new ArrayList<DashboardModels.ReplayRequestSummary>(); }
        try { landing.metrics = metricsToMap(); } catch (Exception ignored) { landing.metrics = new HashMap<String, Object>(); }
        return landing;
    }

    private DashboardModels.HealthResponse degradedKafkaHealth() {
        DashboardModels.HealthResponse response = new DashboardModels.HealthResponse();
        response.generatedAt = Times.nowIsoUtc();
        response.status = "degraded";
        response.cdc = new HashMap<String, Object>();
        response.kafka = new HashMap<String, Object>();
        response.registry = new HashMap<String, Object>();
        response.dlq = new HashMap<String, Object>();
        response.metrics = new HashMap<String, Object>();
        return response;
    }

    private Map<String, Object> registryHealth() {
        Map<String, Object> health = new HashMap<String, Object>();
        health.put("status", "healthy");
        health.put("activeConsumers", consumers().size());
        health.put("routes", routeQueryService.listRoutes().size());
        return health;
    }

    private Map<String, Object> dlqHealth() {
        Map<String, Object> health = new HashMap<String, Object>();
        health.put("status", "healthy");
        health.put("events", dlq(null, "1").size());
        return health;
    }

    private List<DashboardModels.TopicSummary> convertTopics(List<Map<String, Object>> raw) {
        List<DashboardModels.TopicSummary> converted = new ArrayList<DashboardModels.TopicSummary>();
        if (raw == null) return converted;
        for (Map<String, Object> item : raw) {
            try {
                DashboardModels.TopicSummary summary = new DashboardModels.TopicSummary();
                summary.topic = String.valueOf(item.get("topic"));
                summary.partitions = toInt(item.get("partitions"));
                summary.oldestOffset = toLong(item.get("oldestOffset"));
                summary.latestOffset = toLong(item.get("latestOffset"));
                summary.lag = toLong(item.get("consumerLag"));
                summary.messagesPerSecond = 0L;
                converted.add(summary);
            } catch (Exception ignored) {}
        }
        return converted;
    }

    private List<DashboardModels.ConsumerHealthSummary> convertConsumers(List<Map<String, Object>> raw) {
        List<DashboardModels.ConsumerHealthSummary> converted = new ArrayList<DashboardModels.ConsumerHealthSummary>();
        if (raw == null) return converted;
        for (Map<String, Object> item : raw) {
            try {
                DashboardModels.ConsumerHealthSummary summary = new DashboardModels.ConsumerHealthSummary();
                summary.consumer = String.valueOf(item.get("consumer"));
                summary.topic = String.valueOf(item.get("topic"));
                summary.environment = item.containsKey("environment") ? String.valueOf(item.get("environment")) : "unknown";
                summary.status = item.containsKey("status") ? String.valueOf(item.get("status")) : "unknown";
                summary.lag = toLong(item.get("lag"));
                summary.ownerTeam = item.containsKey("ownerTeam") ? String.valueOf(item.get("ownerTeam")) : "unknown";
                summary.updatedAt = item.containsKey("updatedAt") ? String.valueOf(item.get("updatedAt")) : Times.nowIsoUtc();
                converted.add(summary);
            } catch (Exception ignored) {}
        }
        return converted;
    }

    private List<DashboardModels.DlqEventSummary> convertDlq(List<Map<String, Object>> raw) {
        List<DashboardModels.DlqEventSummary> converted = new ArrayList<DashboardModels.DlqEventSummary>();
        for (Map<String, Object> item : raw) {
            DashboardModels.DlqEventSummary summary = new DashboardModels.DlqEventSummary();
            summary.topic = String.valueOf(item.get("topic"));
            summary.partition = String.valueOf(item.get("partition"));
            summary.offset = String.valueOf(item.get("offset"));
            summary.key = String.valueOf(item.get("key"));
            summary.errorType = "unknown";
            summary.errorMessage = String.valueOf(item.get("value"));
            summary.createdAt = Times.nowIsoUtc();
            converted.add(summary);
        }
        return converted;
    }

    private List<DashboardModels.ReplayRequestSummary> convertReplay(List<ReplayRequestRecord> raw) {
        List<DashboardModels.ReplayRequestSummary> converted = new ArrayList<DashboardModels.ReplayRequestSummary>();
        for (ReplayRequestRecord item : raw) {
            DashboardModels.ReplayRequestSummary summary = new DashboardModels.ReplayRequestSummary();
            summary.id = item.id();
            summary.topic = item.topic();
            summary.consumerGroup = item.consumerGroup();
            summary.startTimestamp = item.startTimestamp();
            summary.status = item.status();
            summary.reason = item.reason();
            summary.createdAt = item.createdAt();
            summary.createdBy = item.createdBy();
            converted.add(summary);
        }
        return converted;
    }

    private Map<String, Object> metricsToMap() {
        return platformMetrics.snapshot();
    }

    private long toLong(Object value) {
        if (value instanceof Number) return ((Number) value).longValue();
        return 0L;
    }

    private int toInt(Object value) {
        if (value instanceof Number) return ((Number) value).intValue();
        return 0;
    }
}
