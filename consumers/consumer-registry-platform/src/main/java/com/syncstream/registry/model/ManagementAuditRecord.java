package com.syncstream.registry.model;

public class ManagementAuditRecord {
    private final String id;
    private final String action;
    private final String targetType;
    private final String targetId;
    private final String beforeJson;
    private final String afterJson;
    private final String actor;
    private final String createdAt;

    public ManagementAuditRecord(String id, String action, String targetType, String targetId, String beforeJson, String afterJson, String actor, String createdAt) {
        this.id = id;
        this.action = action;
        this.targetType = targetType;
        this.targetId = targetId;
        this.beforeJson = beforeJson;
        this.afterJson = afterJson;
        this.actor = actor;
        this.createdAt = createdAt;
    }

    public String id() { return id; }
    public String action() { return action; }
    public String targetType() { return targetType; }
    public String targetId() { return targetId; }
    public String beforeJson() { return beforeJson; }
    public String afterJson() { return afterJson; }
    public String actor() { return actor; }
    public String createdAt() { return createdAt; }
}
