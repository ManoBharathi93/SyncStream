package com.syncstream.registry.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class PrometheusService {
    private final String baseUrl;

    public PrometheusService(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public Map<String, Object> health() {
        Map<String, Object> health = new HashMap<String, Object>();
        try {
            String body = get("/-/healthy");
            health.put("status", "healthy");
            health.put("body", body);
        } catch (Exception ex) {
            health.put("status", "degraded");
            health.put("error", ex.getMessage());
        }
        return health;
    }

    public Map<String, Object> query(String expression) {
        try {
            String body = get("/api/v1/query?query=" + java.net.URLEncoder.encode(expression, "UTF-8"));
            Map<String, Object> result = new HashMap<String, Object>();
            result.put("expression", expression);
            result.put("raw", body);
            return result;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to query Prometheus", ex);
        }
    }

    private String get(String path) throws Exception {
        HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl + path).openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(3000);
        connection.setReadTimeout(3000);
        int code = connection.getResponseCode();
        BufferedReader reader = new BufferedReader(new InputStreamReader(
            code >= 200 && code < 300 ? connection.getInputStream() : connection.getErrorStream(),
            StandardCharsets.UTF_8
        ));
        StringBuilder body = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            body.append(line);
        }
        if (code < 200 || code >= 300) {
            throw new IllegalStateException("Prometheus returned HTTP " + code + ": " + body.toString());
        }
        return body.toString();
    }
}
