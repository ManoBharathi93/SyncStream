package com.syncstream.registry.model;

public class RegistryEvent {
    private final String id;
    private final String registrationId;
    private final String eventType;
    private final String payloadJson;
    private final String createdAt;

    public RegistryEvent(String id, String registrationId, String eventType, String payloadJson, String createdAt) {
        this.id = id;
        this.registrationId = registrationId;
        this.eventType = eventType;
        this.payloadJson = payloadJson;
        this.createdAt = createdAt;
    }

    public String id() { return id; }
    public String registrationId() { return registrationId; }
    public String eventType() { return eventType; }
    public String payloadJson() { return payloadJson; }
    public String createdAt() { return createdAt; }
}
