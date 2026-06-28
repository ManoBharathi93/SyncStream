package com.syncstream.registry.service;

import com.syncstream.registry.db.DatabaseManager;
import com.syncstream.registry.db.RegistryRepository;
import com.syncstream.registry.model.ConsumerRegistration;
import com.syncstream.registry.model.RegistrationStatus;
import com.syncstream.registry.model.RegistryEvent;
import com.syncstream.registry.model.RouteState;
import com.syncstream.registry.util.Times;

import java.sql.Connection;
import java.time.Instant;

public class RegistryReconciler implements RegistryEventBus.Listener {
    private final DatabaseManager databaseManager;
    private final RegistryRepository registryRepository;
    private final ConfigurationService configurationService;
    private final RoutingProvisioner routingProvisioner;
    private final PlatformMetrics metrics;

    public RegistryReconciler(
        DatabaseManager databaseManager,
        RegistryRepository registryRepository,
        ConfigurationService configurationService,
        RoutingProvisioner routingProvisioner,
        PlatformMetrics metrics
    ) {
        this.databaseManager = databaseManager;
        this.registryRepository = registryRepository;
        this.configurationService = configurationService;
        this.routingProvisioner = routingProvisioner;
        this.metrics = metrics;
    }

    @Override
    public void onEvent(RegistryEvent event) {
        long startMs = System.currentTimeMillis();
        try (Connection connection = databaseManager.connection()) {
            ConsumerRegistration registration = registryRepository.findById(connection, event.registrationId());
            if (registration == null) {
                return;
            }

            boolean active = registration.status() == RegistrationStatus.PENDING_PROVISIONING
                || registration.status() == RegistrationStatus.ACTIVE;

            RouteState routeState = new RouteState(
                configurationService.routeKey(registration),
                registration.id(),
                registration.consumer(),
                registration.topic(),
                registration.environment(),
                active,
                configurationService.effectiveConfigJson(registration),
                Times.nowIsoUtc()
            );

            routingProvisioner.apply(routeState);

            if (registration.status() == RegistrationStatus.PENDING_PROVISIONING) {
                registryRepository.forceUpdateStatus(connection, registration.id(), RegistrationStatus.ACTIVE, Times.nowIsoUtc(), "registry-reconciler");
            }

            metrics.markProvisioningSuccess();
        } catch (Exception ex) {
            metrics.markProvisioningFailure();
            try (Connection recoveryConnection = databaseManager.connection()) {
                registryRepository.forceUpdateStatus(recoveryConnection, event.registrationId(), RegistrationStatus.DEGRADED, Times.nowIsoUtc(), "registry-reconciler");
            } catch (Exception ignored) {
            }
        } finally {
            long lagMs = Math.max(0, Instant.now().toEpochMilli() - startMs);
            metrics.setReconcileLagMs(lagMs);
        }
    }
}
