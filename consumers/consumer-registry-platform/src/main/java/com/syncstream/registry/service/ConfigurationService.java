package com.syncstream.registry.service;

import com.syncstream.registry.model.ConsumerRegistration;
import com.syncstream.registry.util.Jsons;

import java.util.HashMap;
import java.util.Map;

public class ConfigurationService {

    public String routeKey(ConsumerRegistration registration) {
        return registration.environment() + ":" + registration.topic() + ":" + registration.consumer();
    }

    public String effectiveConfigJson(ConsumerRegistration registration) {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put("consumer", registration.consumer());
        config.put("topic", registration.topic());
        config.put("environment", registration.environment());
        config.put("ownerTeam", registration.ownerTeam());
        config.put("source", "registry-platform");
        config.put("custom", registration.config());
        return Jsons.toJson(config);
    }
}
