package com.syncstream.registry.api;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.syncstream.registry.model.ConsumerRegistration;
import com.syncstream.registry.service.AuthorizationService;
import com.syncstream.registry.service.RegistryException;
import com.syncstream.registry.service.RegistryService;
import com.syncstream.registry.service.ManagementService;
import com.syncstream.registry.service.RouteQueryService;
import com.syncstream.registry.util.Jsons;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class RegistryHttpServer {
    private final HttpServer server;

    public RegistryHttpServer(int port, RegistryService registryService, RouteQueryService routeQueryService, ManagementService managementService, AuthorizationService authorizationService) {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(Executors.newCachedThreadPool());
            server.createContext("/api/v1/consumers", new ConsumerHandler(registryService));
            server.createContext("/api/v1/config/routes", new RoutesHandler(routeQueryService));
            server.createContext("/api/v1/metrics", new MetricsHandler(registryService));
            server.createContext("/api/v1/dashboard", new DashboardApiHandler(managementService, authorizationService));
            server.createContext("/dashboard", new DashboardPageHandler());
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to initialize HTTP server", ex);
        }
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    private static class ConsumerHandler implements HttpHandler {
        private final RegistryService registryService;

        private ConsumerHandler(RegistryService registryService) {
            this.registryService = registryService;
        }

        @Override
        public void handle(HttpExchange exchange) {
            try {
                String method = exchange.getRequestMethod();
                String path = exchange.getRequestURI().getPath();

                if ("POST".equalsIgnoreCase(method) && "/api/v1/consumers".equals(path)) {
                    RegisterConsumerRequest request = Jsons.fromJson(readBody(exchange), RegisterConsumerRequest.class);
                    ConsumerRegistration registration = registryService.register(request);
                    sendJson(exchange, 201, registration);
                    return;
                }

                if ("GET".equalsIgnoreCase(method) && "/api/v1/consumers".equals(path)) {
                    Map<String, String> queryParams = parseQuery(exchange.getRequestURI());
                    sendJson(exchange, 200, registryService.list(queryParams));
                    return;
                }

                if (path.startsWith("/api/v1/consumers/") && path.endsWith("/disable") && "POST".equalsIgnoreCase(method)) {
                    String id = path.substring("/api/v1/consumers/".length(), path.length() - "/disable".length());
                    DisableConsumerRequest request = Jsons.fromJson(readBody(exchange), DisableConsumerRequest.class);
                    sendJson(exchange, 200, registryService.disable(id, request));
                    return;
                }

                if (path.startsWith("/api/v1/consumers/") && path.endsWith("/history") && "GET".equalsIgnoreCase(method)) {
                    String id = path.substring("/api/v1/consumers/".length(), path.length() - "/history".length());
                    sendJson(exchange, 200, registryService.history(id));
                    return;
                }

                if (path.startsWith("/api/v1/consumers/") && "PATCH".equalsIgnoreCase(method)) {
                    String id = path.substring("/api/v1/consumers/".length());
                    UpdateConsumerRequest request = Jsons.fromJson(readBody(exchange), UpdateConsumerRequest.class);
                    sendJson(exchange, 200, registryService.update(id, request));
                    return;
                }

                sendJson(exchange, 404, error("not_found", "Endpoint not found"));
            } catch (RegistryException ex) {
                sendJson(exchange, ex.statusCode(), error("registry_error", ex.getMessage()));
            } catch (Exception ex) {
                sendJson(exchange, 500, error("internal_error", "Internal server error"));
            }
        }
    }

    private static class RoutesHandler implements HttpHandler {
        private final RouteQueryService routeQueryService;

        private RoutesHandler(RouteQueryService routeQueryService) {
            this.routeQueryService = routeQueryService;
        }

        @Override
        public void handle(HttpExchange exchange) {
            try {
                if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                    sendJson(exchange, 405, error("method_not_allowed", "Use GET"));
                    return;
                }
                sendJson(exchange, 200, routeQueryService.listRoutes());
            } catch (Exception ex) {
                sendJson(exchange, 500, error("internal_error", "Internal server error"));
            }
        }
    }

    private static class MetricsHandler implements HttpHandler {
        private final RegistryService registryService;

        private MetricsHandler(RegistryService registryService) {
            this.registryService = registryService;
        }

        @Override
        public void handle(HttpExchange exchange) {
            try {
                if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                    sendJson(exchange, 405, error("method_not_allowed", "Use GET"));
                    return;
                }
                sendJson(exchange, 200, registryService.metrics());
            } catch (Exception ex) {
                sendJson(exchange, 500, error("internal_error", "Internal server error"));
            }
        }
    }

    private static class DashboardApiHandler implements HttpHandler {
        private final ManagementService managementService;
        private final AuthorizationService authorizationService;

        private DashboardApiHandler(ManagementService managementService, AuthorizationService authorizationService) {
            this.managementService = managementService;
            this.authorizationService = authorizationService;
        }

        @Override
        public void handle(HttpExchange exchange) {
            try {
                String method = exchange.getRequestMethod();
                String path = exchange.getRequestURI().getPath();
                String role = exchange.getRequestHeaders().getFirst("X-User-Role");
                String actor = exchange.getRequestHeaders().getFirst("X-User-Name");

                if (path.equals("/api/v1/dashboard") && "GET".equalsIgnoreCase(method)) {
                    authorizationService.requireViewer(role);
                    sendJson(exchange, 200, managementService.dashboardLanding());
                    return;
                }

                if (path.equals("/api/v1/dashboard/health") && "GET".equalsIgnoreCase(method)) {
                    authorizationService.requireViewer(role);
                    sendJson(exchange, 200, managementService.health());
                    return;
                }

                if (path.equals("/api/v1/dashboard/topics") && "GET".equalsIgnoreCase(method)) {
                    authorizationService.requireViewer(role);
                    sendJson(exchange, 200, managementService.topics());
                    return;
                }

                if (path.equals("/api/v1/dashboard/consumers") && "GET".equalsIgnoreCase(method)) {
                    authorizationService.requireViewer(role);
                    sendJson(exchange, 200, managementService.consumers());
                    return;
                }

                if (path.equals("/api/v1/dashboard/dlq") && "GET".equalsIgnoreCase(method)) {
                    authorizationService.requireViewer(role);
                    Map<String, String> queryParams = parseQuery(exchange.getRequestURI());
                    sendJson(exchange, 200, managementService.dlq(queryParams.get("topic"), queryParams.get("limit")));
                    return;
                }

                if (path.equals("/api/v1/dashboard/replay") && "POST".equalsIgnoreCase(method)) {
                    authorizationService.requireAdmin(role);
                    String validatedActor = requireActor(actor);
                    ReplayRequest request = Jsons.fromJson(readBody(exchange), ReplayRequest.class);
                    request.actor = validatedActor;
                    sendJson(exchange, 201, managementService.requestReplay(request));
                    return;
                }

                if (path.equals("/api/v1/dashboard/replay") && "GET".equalsIgnoreCase(method)) {
                    authorizationService.requireViewer(role);
                    sendJson(exchange, 200, managementService.replayRequests());
                    return;
                }

                if (path.equals("/api/v1/dashboard/activity") && "GET".equalsIgnoreCase(method)) {
                    authorizationService.requireAdmin(role);
                    sendJson(exchange, 200, managementService.activity());
                    return;
                }

                if (path.equals("/api/v1/dashboard/metrics") && "GET".equalsIgnoreCase(method)) {
                    authorizationService.requireViewer(role);
                    sendJson(exchange, 200, managementService.metrics());
                    return;
                }

                sendJson(exchange, 404, error("not_found", "Endpoint not found"));
            } catch (RegistryException ex) {
                sendJson(exchange, ex.statusCode(), error("registry_error", ex.getMessage()));
            } catch (Exception ex) {
                sendJson(exchange, 500, error("internal_error", "Internal server error"));
            }
        }

        private String requireActor(String actor) {
            if (actor == null || actor.trim().isEmpty()) {
                throw new RegistryException(400, "actor header required for privileged action");
            }
            return actor.trim();
        }
    }

    private static class DashboardPageHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) {
            try {
                if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                    sendJson(exchange, 405, error("method_not_allowed", "Use GET"));
                    return;
                }
                String html = dashboardHtml();
                byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream outputStream = exchange.getResponseBody()) {
                    outputStream.write(bytes);
                    outputStream.flush();
                }
                exchange.close();
            } catch (Exception ex) {
                sendJson(exchange, 500, error("internal_error", "Internal server error"));
            }
        }
    }

    private static String dashboardHtml() {
        InputStream stream = RegistryHttpServer.class.getClassLoader().getResourceAsStream("dashboard/index.html");
        if (stream == null) {
            return fallbackDashboardHtml();
        }
        StringBuilder html = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                html.append(line).append('\n');
            }
            return html.toString();
        } catch (Exception ex) {
            return fallbackDashboardHtml();
        }
    }

    private static String readBody(HttpExchange exchange) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
            StringBuilder body = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                body.append(line);
            }
            return body.toString();
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to read request body", ex);
        }
    }

    private static String fallbackDashboardHtml() {
        return "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"><title>SyncStream Admin Dashboard</title><style>body{font-family:Segoe UI,sans-serif;margin:0;padding:24px;background:#07111f;color:#edf4ff}pre{background:#0d1930;border:1px solid #22395f;padding:12px;border-radius:8px;overflow:auto}</style></head><body><h1>SyncStream Admin Dashboard</h1><p>Minimal fallback UI loaded.</p><pre id=\"out\">Loading /api/v1/dashboard ...</pre><script>fetch('/api/v1/dashboard',{headers:{'X-User-Role':'admin','X-User-Name':'dashboard-user'}}).then(function(r){return r.json()}).then(function(d){document.getElementById('out').textContent=JSON.stringify(d,null,2)}).catch(function(e){document.getElementById('out').textContent='Failed to load dashboard API: '+e.message});</script></body></html>";
    }

    private static void sendJson(HttpExchange exchange, int code, Object payload) {
        try {
            byte[] bytes = Jsons.toJson(payload).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(code, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to write HTTP response", ex);
        }
    }

    private static Map<String, String> parseQuery(URI uri) {
        Map<String, String> query = new HashMap<String, String>();
        if (uri.getQuery() == null || uri.getQuery().trim().isEmpty()) {
            return query;
        }
        String[] tokens = uri.getQuery().split("&");
        for (String token : tokens) {
            String[] kv = token.split("=", 2);
            String key = kv[0];
            String value = kv.length > 1 ? kv[1] : "";
            query.put(key, value);
        }
        return query;
    }

    private static Map<String, String> error(String code, String message) {
        Map<String, String> payload = new HashMap<String, String>();
        payload.put("code", code);
        payload.put("message", message == null ? "" : message);
        return payload;
    }
}
