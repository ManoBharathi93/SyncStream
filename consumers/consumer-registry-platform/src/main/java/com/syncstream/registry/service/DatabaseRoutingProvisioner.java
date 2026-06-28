package com.syncstream.registry.service;

import com.syncstream.registry.db.DatabaseManager;
import com.syncstream.registry.db.RouteStateRepository;
import com.syncstream.registry.model.RouteState;

import java.sql.Connection;

public class DatabaseRoutingProvisioner implements RoutingProvisioner {
    private final DatabaseManager databaseManager;
    private final RouteStateRepository routeStateRepository;

    public DatabaseRoutingProvisioner(DatabaseManager databaseManager, RouteStateRepository routeStateRepository) {
        this.databaseManager = databaseManager;
        this.routeStateRepository = routeStateRepository;
    }

    @Override
    public void apply(RouteState routeState) {
        try (Connection connection = databaseManager.connection()) {
            routeStateRepository.upsert(connection, routeState);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to apply route provisioning", ex);
        }
    }
}
