package com.syncstream.elasticsearchsync.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class DebeziumEnvelope {
    private JsonNode before;
    private JsonNode after;
    private String op;
    private JsonNode source;
    private Long ts_ms;

    public DebeziumEnvelope() {
    }

    public JsonNode getBefore() {
        return before;
    }

    public void setBefore(JsonNode before) {
        this.before = before;
    }

    public JsonNode getAfter() {
        return after;
    }

    public void setAfter(JsonNode after) {
        this.after = after;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public JsonNode getSource() {
        return source;
    }

    public void setSource(JsonNode source) {
        this.source = source;
    }

    public Long getTs_ms() {
        return ts_ms;
    }

    public void setTs_ms(Long ts_ms) {
        this.ts_ms = ts_ms;
    }

    public JsonNode before() {
        return before;
    }

    public JsonNode after() {
        return after;
    }

    public String op() {
        return op;
    }

    public JsonNode source() {
        return source;
    }

    public Long tsMs() {
        return ts_ms;
    }

    public boolean isDelete() {
        return "d".equals(op);
    }
}
