package com.syncstream.registry.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ConsumerRegistration {
    private final String id;
    private final String consumer;
    private final String topic;
    private final String environment;
    private final String ownerTeam;
    private final RegistrationStatus status;
    private final Map<String, Object> config;
    private final long version;
    private final String createdAt;
    private final String updatedAt;
    private final String createdBy;
    private final String updatedBy;

    public ConsumerRegistration(
        String id,
        String consumer,
        String topic,
        String environment,
        String ownerTeam,
        RegistrationStatus status,
        Map<String, Object> config,
        long version,
        String createdAt,
        String updatedAt,
        String createdBy,
        String updatedBy
    ) {
        this.id = id;
        this.consumer = consumer;
        this.topic = topic;
        this.environment = environment;
        this.ownerTeam = ownerTeam;
        this.status = status;
        this.config = Collections.unmodifiableMap(new HashMap<String, Object>(config));
        this.version = version;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.createdBy = createdBy;
        this.updatedBy = updatedBy;
    }

    public String id() { return id; }
    public String consumer() { return consumer; }
    public String topic() { return topic; }
    public String environment() { return environment; }
    public String ownerTeam() { return ownerTeam; }
    public RegistrationStatus status() { return status; }
    public Map<String, Object> config() { return config; }
    public long version() { return version; }
    public String createdAt() { return createdAt; }
    public String updatedAt() { return updatedAt; }
    public String createdBy() { return createdBy; }
    public String updatedBy() { return updatedBy; }
}
