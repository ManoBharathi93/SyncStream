package com.syncstream.registry.api;

public class ReplayRequest {
    public String topic;
    public String consumerGroup;
    public String startTimestamp;
    public String startOffset;
    public String endOffset;
    public String reason;
    public String actor;
}
