package com.syncstream.registry.model;

public class RouteState {
    private final String routeKey;
    private final String registrationId;
    private final String consumer;
    private final String topic;
    private final String environment;
    private final boolean active;
    private final String effectiveConfigJson;
    private final String updatedAt;

    public RouteState(
        String routeKey,
        String registrationId,
        String consumer,
        String topic,
        String environment,
        boolean active,
        String effectiveConfigJson,
        String updatedAt
    ) {
        this.routeKey = routeKey;
        this.registrationId = registrationId;
        this.consumer = consumer;
        this.topic = topic;
        this.environment = environment;
        this.active = active;
        this.effectiveConfigJson = effectiveConfigJson;
        this.updatedAt = updatedAt;
    }

    public String routeKey() { return routeKey; }
    public String registrationId() { return registrationId; }
    public String consumer() { return consumer; }
    public String topic() { return topic; }
    public String environment() { return environment; }
    public boolean active() { return active; }
    public String effectiveConfigJson() { return effectiveConfigJson; }
    public String updatedAt() { return updatedAt; }
}
