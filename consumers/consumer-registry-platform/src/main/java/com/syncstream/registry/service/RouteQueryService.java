package com.syncstream.registry.service;

import com.syncstream.registry.db.DatabaseManager;
import com.syncstream.registry.db.RouteStateRepository;
import com.syncstream.registry.model.RouteState;

import java.sql.Connection;
import java.util.List;

public class RouteQueryService {
    private final DatabaseManager databaseManager;
    private final RouteStateRepository routeStateRepository;

    public RouteQueryService(DatabaseManager databaseManager, RouteStateRepository routeStateRepository) {
        this.databaseManager = databaseManager;
        this.routeStateRepository = routeStateRepository;
    }

    public List<RouteState> listRoutes() {
        try (Connection connection = databaseManager.connection()) {
            return routeStateRepository.list(connection);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to list routes", ex);
        }
    }
}
