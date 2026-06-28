package com.syncstream.registry.app;

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

public class RegistryPlatformBootstrap {
    public static void main(String[] args) {
        String jdbcUrl = System.getenv().getOrDefault("REGISTRY_DB_URL", "jdbc:sqlite:consumer_registry.db");
        int port = Integer.parseInt(System.getenv().getOrDefault("REGISTRY_PORT", "8091"));

        DatabaseManager databaseManager = new DatabaseManager(jdbcUrl);
        databaseManager.initialize();
        new MigrationRunner(databaseManager).migrate();

        RegistryRepository registryRepository = new RegistryRepository();
        AuditRepository auditRepository = new AuditRepository();
        EventRepository eventRepository = new EventRepository();
        RouteStateRepository routeStateRepository = new RouteStateRepository();
        ReplayRepository replayRepository = new ReplayRepository();
        ManagementAuditRepository managementAuditRepository = new ManagementAuditRepository();

        PlatformMetrics metrics = new PlatformMetrics();
        RegistryEventBus eventBus = new RegistryEventBus();
        ConfigurationService configurationService = new ConfigurationService();

        RegistryService registryService = new RegistryService(
            databaseManager,
            registryRepository,
            auditRepository,
            eventRepository,
            new MetadataValidator(),
            eventBus,
            metrics
        );

        RegistryReconciler reconciler = new RegistryReconciler(
            databaseManager,
            registryRepository,
            configurationService,
            new DatabaseRoutingProvisioner(databaseManager, routeStateRepository),
            metrics
        );
        eventBus.register(reconciler);

        RouteQueryService routeQueryService = new RouteQueryService(databaseManager, routeStateRepository);
        KafkaMetadataService kafkaMetadataService = new KafkaMetadataService(System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        PrometheusService prometheusService = new PrometheusService(System.getenv().getOrDefault("PROMETHEUS_URL", "http://localhost:9090"));
        ManagementService managementService = new ManagementService(
            databaseManager,
            registryService,
            routeQueryService,
            kafkaMetadataService,
            prometheusService,
            replayRepository,
            managementAuditRepository,
            metrics
        );

        RegistryHttpServer server = new RegistryHttpServer(port, registryService, routeQueryService, managementService, new AuthorizationService());
        server.start();

        System.out.println("Consumer Registry Platform started on port " + port + " (db=" + jdbcUrl + ")");
    }
}
