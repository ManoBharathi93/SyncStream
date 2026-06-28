package com.syncstream.registry.api;

import java.util.HashMap;
import java.util.Map;

public class UpdateConsumerRequest {
    public String ownerTeam;
    public String actor;
    public Long expectedVersion;
    public String status;
    public Map<String, Object> config = new HashMap<String, Object>();
}
