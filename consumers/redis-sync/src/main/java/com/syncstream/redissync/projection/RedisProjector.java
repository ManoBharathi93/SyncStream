package com.syncstream.redissync.projection;

import com.syncstream.redissync.model.DebeziumEnvelope;

public interface RedisProjector {
    void apply(String topic, String keyJson, DebeziumEnvelope event);
}
