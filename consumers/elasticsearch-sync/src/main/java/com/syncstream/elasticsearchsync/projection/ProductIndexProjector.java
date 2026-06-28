package com.syncstream.elasticsearchsync.projection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syncstream.elasticsearchsync.model.DebeziumEnvelope;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

public class ProductIndexProjector implements ElasticsearchProjector {
    private final RestClient client;
    private final ObjectMapper mapper;
    private final String indexName;

    public ProductIndexProjector(RestClient client, ObjectMapper mapper, String indexName) {
        this.client = client;
        this.mapper = mapper;
        this.indexName = indexName;
    }

    @Override
    public void apply(String topic, String keyJson, DebeziumEnvelope event) throws Exception {
        if (!topic.endsWith(".products")) {
            return;
        }

        String id = extractId(keyJson);
        if (id == null) {
            return;
        }

        if (event.isDelete()) {
            Request deleteRequest = new Request("DELETE", "/" + indexName + "/_doc/" + id);
            Response response = client.performRequest(deleteRequest);
            int status = response.getStatusLine().getStatusCode();
            if (status != 200 && status != 404) {
                throw new IllegalStateException("Unexpected delete status " + status + " for id=" + id);
            }
            return;
        }

        JsonNode after = event.after();
        if (after == null || after.isNull()) {
            return;
        }

        Request upsertRequest = new Request("PUT", "/" + indexName + "/_doc/" + id);
        upsertRequest.setJsonEntity(after.toString());
        Response response = client.performRequest(upsertRequest);
        int status = response.getStatusLine().getStatusCode();
        if (status != 200 && status != 201) {
            throw new IllegalStateException("Unexpected upsert status " + status + " for id=" + id);
        }
    }

    private String extractId(String keyJson) {
        if (keyJson == null) {
            return null;
        }

        try {
            JsonNode keyNode = mapper.readTree(keyJson);
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
        } catch (Exception ex) {
            return null;
        }
    }
}
