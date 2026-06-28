package com.syncstream.registry;

import com.syncstream.registry.db.AuditRepository;
import com.syncstream.registry.db.DatabaseManager;
import com.syncstream.registry.db.EventRepository;
import com.syncstream.registry.db.MigrationRunner;
import com.syncstream.registry.db.RegistryRepository;
import com.syncstream.registry.db.RouteStateRepository;
import com.syncstream.registry.service.ConfigurationService;
import com.syncstream.registry.service.DatabaseRoutingProvisioner;
import com.syncstream.registry.service.MetadataValidator;
import com.syncstream.registry.service.PlatformMetrics;
import com.syncstream.registry.service.RegistryEventBus;
import com.syncstream.registry.service.RegistryReconciler;
import com.syncstream.registry.service.RegistryService;
import com.syncstream.registry.service.RouteQueryService;

import java.io.File;
import java.util.UUID;

public final class TestBootstrap {
    private TestBootstrap() {
    }

    public static class Context {
        public final DatabaseManager databaseManager;
        public final RegistryService registryService;
        public final RouteQueryService routeQueryService;
        public final PlatformMetrics metrics;
        public final RegistryRepository registryRepository;
        public final RegistryEventBus eventBus;

        private Context(
            DatabaseManager databaseManager,
            RegistryService registryService,
            RouteQueryService routeQueryService,
            PlatformMetrics metrics,
            RegistryRepository registryRepository,
            RegistryEventBus eventBus
        ) {
            this.databaseManager = databaseManager;
            this.registryService = registryService;
            this.routeQueryService = routeQueryService;
            this.metrics = metrics;
            this.registryRepository = registryRepository;
            this.eventBus = eventBus;
        }
    }

    public static Context context() {
        String dbFile = "target/test-db-" + UUID.randomUUID().toString() + ".sqlite";
        new File("target").mkdirs();
        DatabaseManager databaseManager = new DatabaseManager("jdbc:sqlite:" + dbFile);
        databaseManager.initialize();
        new MigrationRunner(databaseManager).migrate();

        RegistryRepository registryRepository = new RegistryRepository();
        AuditRepository auditRepository = new AuditRepository();
        EventRepository eventRepository = new EventRepository();
        RouteStateRepository routeStateRepository = new RouteStateRepository();

        PlatformMetrics metrics = new PlatformMetrics();
        RegistryEventBus eventBus = new RegistryEventBus();

        RegistryService service = new RegistryService(
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
            new ConfigurationService(),
            new DatabaseRoutingProvisioner(databaseManager, routeStateRepository),
            metrics
        );

        eventBus.register(reconciler);

        RouteQueryService routeQueryService = new RouteQueryService(databaseManager, routeStateRepository);
        return new Context(databaseManager, service, routeQueryService, metrics, registryRepository, eventBus);
    }
}
