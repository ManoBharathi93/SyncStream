package com.syncstream.registry.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.util.Collections;
import java.util.Map;

public final class Jsons {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    private Jsons() {
    }

    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize JSON", ex);
        }
    }

    public static <T> T fromJson(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Invalid JSON payload", ex);
        }
    }

    public static Map<String, Object> asMap(String json) {
        try {
            if (json == null || json.trim().isEmpty()) {
                return Collections.emptyMap();
            }
            return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() { });
        } catch (Exception ex) {
            throw new IllegalArgumentException("Invalid JSON object payload", ex);
        }
    }
}
