package com.syncstream.registry;

import com.syncstream.registry.api.RegisterConsumerRequest;
import com.syncstream.registry.db.AuditRepository;
import com.syncstream.registry.db.DatabaseManager;
import com.syncstream.registry.db.EventRepository;
import com.syncstream.registry.db.MigrationRunner;
import com.syncstream.registry.db.RegistryRepository;
import com.syncstream.registry.model.ConsumerRegistration;
import com.syncstream.registry.model.RouteState;
import com.syncstream.registry.service.ConfigurationService;
import com.syncstream.registry.service.MetadataValidator;
import com.syncstream.registry.service.PlatformMetrics;
import com.syncstream.registry.service.RegistryEventBus;
import com.syncstream.registry.service.RegistryReconciler;
import com.syncstream.registry.service.RegistryService;
import com.syncstream.registry.service.RoutingProvisioner;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureInjectionTest {

    @Test
    void reconcilerShouldMarkMetricsOnProvisioningFailure() throws Exception {
        new File("target").mkdirs();
        DatabaseManager databaseManager = new DatabaseManager("jdbc:sqlite:target/test-failure-" + UUID.randomUUID() + ".sqlite");
        databaseManager.initialize();
        new MigrationRunner(databaseManager).migrate();

        RegistryRepository registryRepository = new RegistryRepository();
        PlatformMetrics metrics = new PlatformMetrics();
        RegistryEventBus eventBus = new RegistryEventBus();

        RegistryService service = new RegistryService(
            databaseManager,
            registryRepository,
            new AuditRepository(),
            new EventRepository(),
            new MetadataValidator(),
            eventBus,
            metrics
        );

        RoutingProvisioner failingProvisioner = new RoutingProvisioner() {
            @Override
            public void apply(RouteState routeState) {
                throw new IllegalStateException("Injected failure");
            }
        };

        eventBus.register(new RegistryReconciler(
            databaseManager,
            registryRepository,
            new ConfigurationService(),
            failingProvisioner,
            metrics
        ));

        RegisterConsumerRequest request = new RegisterConsumerRequest();
        request.consumer = "analytics-failure";
        request.topic = "syncstream.orders";
        request.environment = "dev";
        request.ownerTeam = "team-analytics";
        request.actor = "qa";

        ConsumerRegistration created = service.register(request);

        try (Connection connection = databaseManager.connection()) {
            ConsumerRegistration post = registryRepository.findById(connection, created.id());
            assertEquals("DEGRADED", post.status().name());
        }

        Number failureCount = (Number) metrics.snapshot().get("provisioningFailureCount");
        assertTrue(failureCount.longValue() >= 1);
    }
}
