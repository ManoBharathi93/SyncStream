package com.syncstream.elasticsearchsync.projection;

import com.syncstream.elasticsearchsync.model.DebeziumEnvelope;

public interface ElasticsearchProjector {
    void apply(String topic, String keyJson, DebeziumEnvelope event) throws Exception;
}
