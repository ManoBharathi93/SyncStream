package com.syncstream.redissync.projection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncstream.redissync.model.DebeziumEnvelope;
import redis.clients.jedis.JedisPooled;

public class ProductCacheProjector implements RedisProjector {
    private final JedisPooled redis;
    private final ObjectMapper mapper;

    public ProductCacheProjector(JedisPooled redis, ObjectMapper mapper) {
        this.redis = redis;
        this.mapper = mapper;
    }

    @Override
    public void apply(String topic, String keyJson, DebeziumEnvelope event) {
        if (!topic.endsWith(".products")) {
            return;
        }

        String id = extractId(keyJson);
        if (id == null) {
            return;
        }

        String redisKey = "cache:products:" + id;

        if (event.isDelete()) {
            redis.del(redisKey);
            return;
        }

        JsonNode current = event.after();
        if (current == null || current.isNull()) {
            return;
        }

        redis.set(redisKey, current.toString());
    }

    private JsonNode parseJson(String json) {
        try {
            return mapper.readTree(json);
        } catch (Exception ex) {
            return mapper.createObjectNode();
        }
    }

    private String extractId(String keyJson) {
        if (keyJson == null) {
            return null;
        }

        JsonNode keyNode = parseJson(keyJson);
        if (keyNode.isObject() && keyNode.has("payload")) {
            keyNode = keyNode.get("payload");
        }

        if (keyNode.isObject()) {
            JsonNode idNode = keyNode.path("id");
            if (idNode.isMissingNode() || idNode.isNull()) {
                return null;
            }
            return idNode.asText();
        }

        if (keyNode.isValueNode()) {
            return keyNode.asText();
        }

        return null;
    }
}
