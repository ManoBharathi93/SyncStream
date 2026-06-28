package com.syncstream.registry.model;

public class RegistrationAuditRecord {
    private final String id;
    private final String registrationId;
    private final String action;
    private final String beforeJson;
    private final String afterJson;
    private final String actor;
    private final String createdAt;

    public RegistrationAuditRecord(
        String id,
        String registrationId,
        String action,
        String beforeJson,
        String afterJson,
        String actor,
        String createdAt
    ) {
        this.id = id;
        this.registrationId = registrationId;
        this.action = action;
        this.beforeJson = beforeJson;
        this.afterJson = afterJson;
        this.actor = actor;
        this.createdAt = createdAt;
    }

    public String id() { return id; }
    public String registrationId() { return registrationId; }
    public String action() { return action; }
    public String beforeJson() { return beforeJson; }
    public String afterJson() { return afterJson; }
    public String actor() { return actor; }
    public String createdAt() { return createdAt; }
}
