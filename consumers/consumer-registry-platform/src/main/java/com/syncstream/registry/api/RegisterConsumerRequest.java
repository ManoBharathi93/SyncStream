package com.syncstream.registry.api;

import java.util.HashMap;
import java.util.Map;

public class RegisterConsumerRequest {
    public String consumer;
    public String topic;
    public String environment;
    public String ownerTeam;
    public String actor;
    public Map<String, Object> config = new HashMap<String, Object>();
}
