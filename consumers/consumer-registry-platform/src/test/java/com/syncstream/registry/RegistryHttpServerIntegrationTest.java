package com.syncstream.registry;

import com.syncstream.registry.api.RegisterConsumerRequest;
import com.syncstream.registry.api.ReplayRequest;
import com.syncstream.registry.api.RegistryHttpServer;
import com.syncstream.registry.db.AuditRepository;
import com.syncstream.registry.db.DatabaseManager;
import com.syncstream.registry.db.EventRepository;
import com.syncstream.registry.db.MigrationRunner;
import com.syncstream.registry.db.ManagementAuditRepository;
import com.syncstream.registry.db.ReplayRepository;
import com.syncstream.registry.db.RegistryRepository;
import com.syncstream.registry.db.RouteStateRepository;
import com.syncstream.registry.service.AuthorizationService;
import com.syncstream.registry.service.ConfigurationService;
import com.syncstream.registry.service.DatabaseRoutingProvisioner;
import com.syncstream.registry.service.KafkaMetadataService;
import com.syncstream.registry.service.MetadataValidator;
import com.syncstream.registry.service.ManagementService;
import com.syncstream.registry.service.PlatformMetrics;
import com.syncstream.registry.service.PrometheusService;
import com.syncstream.registry.service.RegistryEventBus;
import com.syncstream.registry.service.RegistryReconciler;
import com.syncstream.registry.service.RegistryService;
import com.syncstream.registry.service.RouteQueryService;
import com.syncstream.registry.util.Jsons;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RegistryHttpServerIntegrationTest {

    private static class RunningServer {
        private final RegistryHttpServer server;
        private final int port;

        private RunningServer(RegistryHttpServer server, int port) {
            this.server = server;
            this.port = port;
        }
    }

    private RunningServer startServer() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }

        new File("target").mkdirs();
        DatabaseManager databaseManager = new DatabaseManager("jdbc:sqlite:target/test-http-" + UUID.randomUUID() + ".sqlite");
        databaseManager.initialize();
        new MigrationRunner(databaseManager).migrate();

        RegistryRepository registryRepository = new RegistryRepository();
        RouteStateRepository routeStateRepository = new RouteStateRepository();
        PlatformMetrics metrics = new PlatformMetrics();
        RegistryEventBus eventBus = new RegistryEventBus();
        ReplayRepository replayRepository = new ReplayRepository();
        ManagementAuditRepository managementAuditRepository = new ManagementAuditRepository();

        RegistryService service = new RegistryService(
            databaseManager,
            registryRepository,
            new AuditRepository(),
            new EventRepository(),
            new MetadataValidator(),
            eventBus,
            metrics
        );

        eventBus.register(new RegistryReconciler(
            databaseManager,
            registryRepository,
            new ConfigurationService(),
            new DatabaseRoutingProvisioner(databaseManager, routeStateRepository),
            metrics
        ));

        RouteQueryService routeQueryService = new RouteQueryService(databaseManager, routeStateRepository);
        ManagementService managementService = new ManagementService(
            databaseManager,
            service,
            routeQueryService,
            new KafkaMetadataService("localhost:65535"),
            new PrometheusService("http://localhost:65535"),
            replayRepository,
            managementAuditRepository,
            metrics
        );

        RegistryHttpServer server = new RegistryHttpServer(port, service, routeQueryService, managementService, new AuthorizationService());
        server.start();
        return new RunningServer(server, port);
    }

    @Test
    void shouldRegisterAndListUsingHttpApi() throws Exception {
        RunningServer running = startServer();
        try {
            RegisterConsumerRequest request = new RegisterConsumerRequest();
            request.consumer = "analytics-http";
            request.topic = "syncstream.orders";
            request.environment = "dev";
            request.ownerTeam = "team-analytics";
            request.actor = "team-analytics";

            HttpURLConnection register = (HttpURLConnection) new URL("http://localhost:" + running.port + "/api/v1/consumers").openConnection();
            register.setRequestMethod("POST");
            register.setDoOutput(true);
            register.setRequestProperty("Content-Type", "application/json");
            try (OutputStream outputStream = register.getOutputStream()) {
                outputStream.write(Jsons.toJson(request).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(201, register.getResponseCode());

            HttpURLConnection list = (HttpURLConnection) new URL("http://localhost:" + running.port + "/api/v1/consumers").openConnection();
            list.setRequestMethod("GET");
            assertEquals(200, list.getResponseCode());

            HttpURLConnection metricsConn = (HttpURLConnection) new URL("http://localhost:" + running.port + "/api/v1/metrics").openConnection();
            metricsConn.setRequestMethod("GET");
            assertEquals(200, metricsConn.getResponseCode());

            HttpURLConnection routeConn = (HttpURLConnection) new URL("http://localhost:" + running.port + "/api/v1/config/routes").openConnection();
            routeConn.setRequestMethod("GET");
            assertEquals(200, routeConn.getResponseCode());

            HttpURLConnection dashboard = (HttpURLConnection) new URL("http://localhost:" + running.port + "/api/v1/dashboard").openConnection();
            dashboard.setRequestMethod("GET");
            dashboard.setRequestProperty("X-User-Role", "admin");
            assertEquals(200, dashboard.getResponseCode());

            HttpURLConnection dashboardPage = (HttpURLConnection) new URL("http://localhost:" + running.port + "/dashboard").openConnection();
            dashboardPage.setRequestMethod("GET");
            assertEquals(200, dashboardPage.getResponseCode());
        } finally {
            running.server.stop();
        }
    }

    @Test
    void shouldEnforceRoleAllowlistAndPrivilegedActorHeader() throws Exception {
        RunningServer running = startServer();
        try {
            HttpURLConnection dashboardUnknownRole = (HttpURLConnection) new URL("http://localhost:" + running.port + "/api/v1/dashboard/health").openConnection();
            dashboardUnknownRole.setRequestMethod("GET");
            dashboardUnknownRole.setRequestProperty("X-User-Role", "unknown-role");
            assertEquals(403, dashboardUnknownRole.getResponseCode());

            HttpURLConnection dashboardViewerRole = (HttpURLConnection) new URL("http://localhost:" + running.port + "/api/v1/dashboard/health").openConnection();
            dashboardViewerRole.setRequestMethod("GET");
            dashboardViewerRole.setRequestProperty("X-User-Role", "viewer");
            assertEquals(200, dashboardViewerRole.getResponseCode());

            ReplayRequest replayRequest = new ReplayRequest();
            replayRequest.topic = "syncstream.orders";
            replayRequest.consumerGroup = "analytics-http";
            replayRequest.startOffset = "0";
            replayRequest.endOffset = "100";
            replayRequest.reason = "integration-test";

            HttpURLConnection replayMissingActor = (HttpURLConnection) new URL("http://localhost:" + running.port + "/api/v1/dashboard/replay").openConnection();
            replayMissingActor.setRequestMethod("POST");
            replayMissingActor.setDoOutput(true);
            replayMissingActor.setRequestProperty("Content-Type", "application/json");
            replayMissingActor.setRequestProperty("X-User-Role", "admin");
            try (OutputStream outputStream = replayMissingActor.getOutputStream()) {
                outputStream.write(Jsons.toJson(replayRequest).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(400, replayMissingActor.getResponseCode());

            HttpURLConnection replayWithActor = (HttpURLConnection) new URL("http://localhost:" + running.port + "/api/v1/dashboard/replay").openConnection();
            replayWithActor.setRequestMethod("POST");
            replayWithActor.setDoOutput(true);
            replayWithActor.setRequestProperty("Content-Type", "application/json");
            replayWithActor.setRequestProperty("X-User-Role", "admin");
            replayWithActor.setRequestProperty("X-User-Name", "integration-admin");
            try (OutputStream outputStream = replayWithActor.getOutputStream()) {
                outputStream.write(Jsons.toJson(replayRequest).getBytes(StandardCharsets.UTF_8));
            }
            assertEquals(201, replayWithActor.getResponseCode());
        } finally {
            running.server.stop();
        }
    }
}
