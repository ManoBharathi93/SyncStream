package com.syncstream.registry.db;

import com.syncstream.registry.model.RegistryEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class EventRepository {
    public void insert(Connection connection, RegistryEvent event) {
        String sql = "INSERT INTO registry_events(id, registration_id, event_type, payload_json, created_at) VALUES(?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, event.id());
            statement.setString(2, event.registrationId());
            statement.setString(3, event.eventType());
            statement.setString(4, event.payloadJson());
            statement.setString(5, event.createdAt());
            statement.executeUpdate();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to insert registry event", ex);
        }
    }
}
