package com.syncstream.registry.db;

import com.syncstream.registry.model.RouteState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class RouteStateRepository {
    public void upsert(Connection connection, RouteState routeState) {
        String sql = "INSERT INTO routing_state(route_key, registration_id, consumer, topic, environment, active, effective_config_json, updated_at) " +
            "VALUES(?,?,?,?,?,?,?,?) " +
            "ON CONFLICT(route_key) DO UPDATE SET registration_id=excluded.registration_id, consumer=excluded.consumer, topic=excluded.topic, " +
            "environment=excluded.environment, active=excluded.active, effective_config_json=excluded.effective_config_json, updated_at=excluded.updated_at";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, routeState.routeKey());
            statement.setString(2, routeState.registrationId());
            statement.setString(3, routeState.consumer());
            statement.setString(4, routeState.topic());
            statement.setString(5, routeState.environment());
            statement.setInt(6, routeState.active() ? 1 : 0);
            statement.setString(7, routeState.effectiveConfigJson());
            statement.setString(8, routeState.updatedAt());
            statement.executeUpdate();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to upsert route state", ex);
        }
    }

    public List<RouteState> list(Connection connection) {
        String sql = "SELECT * FROM routing_state ORDER BY updated_at DESC";
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            List<RouteState> result = new ArrayList<RouteState>();
            while (rs.next()) {
                result.add(new RouteState(
                    rs.getString("route_key"),
                    rs.getString("registration_id"),
                    rs.getString("consumer"),
                    rs.getString("topic"),
                    rs.getString("environment"),
                    rs.getInt("active") == 1,
                    rs.getString("effective_config_json"),
                    rs.getString("updated_at")
                ));
            }
            return result;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to list route state", ex);
        }
    }
}
