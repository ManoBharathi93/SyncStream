package com.syncstream.registry.model;

public class ReplayRequestRecord {
    private final String id;
    private final String topic;
    private final String consumerGroup;
    private final String startTimestamp;
    private final String startOffset;
    private final String endOffset;
    private final String reason;
    private final String status;
    private final String createdAt;
    private final String createdBy;
    private final String updatedAt;
    private final String updatedBy;

    public ReplayRequestRecord(
        String id,
        String topic,
        String consumerGroup,
        String startTimestamp,
        String startOffset,
        String endOffset,
        String reason,
        String status,
        String createdAt,
        String createdBy,
        String updatedAt,
        String updatedBy
    ) {
        this.id = id;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.startTimestamp = startTimestamp;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.reason = reason;
        this.status = status;
        this.createdAt = createdAt;
        this.createdBy = createdBy;
        this.updatedAt = updatedAt;
        this.updatedBy = updatedBy;
    }

    public String id() { return id; }
    public String topic() { return topic; }
    public String consumerGroup() { return consumerGroup; }
    public String startTimestamp() { return startTimestamp; }
    public String startOffset() { return startOffset; }
    public String endOffset() { return endOffset; }
    public String reason() { return reason; }
    public String status() { return status; }
    public String createdAt() { return createdAt; }
    public String createdBy() { return createdBy; }
    public String updatedAt() { return updatedAt; }
    public String updatedBy() { return updatedBy; }
}
