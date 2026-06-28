package com.syncstream.registry.db;

import com.syncstream.registry.model.ConsumerRegistration;
import com.syncstream.registry.model.RegistrationStatus;
import com.syncstream.registry.util.Jsons;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RegistryRepository {

    public ConsumerRegistration findByUnique(Connection connection, String consumer, String topic, String environment) {
        String sql = "SELECT * FROM consumer_registrations WHERE consumer=? AND topic=? AND environment=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, consumer);
            statement.setString(2, topic);
            statement.setString(3, environment);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return mapRegistration(rs);
            }
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to query registration by unique key", ex);
        }
    }

    public ConsumerRegistration findById(Connection connection, String id) {
        String sql = "SELECT * FROM consumer_registrations WHERE id=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, id);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return mapRegistration(rs);
            }
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to query registration by id", ex);
        }
    }

    public void insert(Connection connection, ConsumerRegistration registration) {
        String sql = "INSERT INTO consumer_registrations(id, consumer, topic, environment, owner_team, status, config_json, version, created_at, updated_at, created_by, updated_by) " +
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, registration.id());
            statement.setString(2, registration.consumer());
            statement.setString(3, registration.topic());
            statement.setString(4, registration.environment());
            statement.setString(5, registration.ownerTeam());
            statement.setString(6, registration.status().toDbValue());
            statement.setString(7, Jsons.toJson(registration.config()));
            statement.setLong(8, registration.version());
            statement.setString(9, registration.createdAt());
            statement.setString(10, registration.updatedAt());
            statement.setString(11, registration.createdBy());
            statement.setString(12, registration.updatedBy());
            statement.executeUpdate();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to insert registration", ex);
        }
    }

    public boolean updateWithVersion(Connection connection, ConsumerRegistration registration, long expectedVersion) {
        String sql = "UPDATE consumer_registrations SET owner_team=?, status=?, config_json=?, version=?, updated_at=?, updated_by=? WHERE id=? AND version=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, registration.ownerTeam());
            statement.setString(2, registration.status().toDbValue());
            statement.setString(3, Jsons.toJson(registration.config()));
            statement.setLong(4, registration.version());
            statement.setString(5, registration.updatedAt());
            statement.setString(6, registration.updatedBy());
            statement.setString(7, registration.id());
            statement.setLong(8, expectedVersion);
            return statement.executeUpdate() == 1;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to update registration", ex);
        }
    }

    public void forceUpdateStatus(Connection connection, String id, RegistrationStatus status, String updatedAt, String updatedBy) {
        String sql = "UPDATE consumer_registrations SET status=?, version=version+1, updated_at=?, updated_by=? WHERE id=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, status.toDbValue());
            statement.setString(2, updatedAt);
            statement.setString(3, updatedBy);
            statement.setString(4, id);
            statement.executeUpdate();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to force update registration status", ex);
        }
    }

    public List<ConsumerRegistration> list(Connection connection, Map<String, String> filters) {
        StringBuilder sql = new StringBuilder("SELECT * FROM consumer_registrations WHERE 1=1");
        List<String> args = new ArrayList<String>();

        addFilter(sql, args, filters, "topic", "topic");
        addFilter(sql, args, filters, "ownerTeam", "owner_team");
        addFilter(sql, args, filters, "status", "status");
        addFilter(sql, args, filters, "environment", "environment");
        sql.append(" ORDER BY created_at ASC");

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < args.size(); i++) {
                statement.setString(i + 1, args.get(i));
            }
            List<ConsumerRegistration> result = new ArrayList<ConsumerRegistration>();
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    result.add(mapRegistration(rs));
                }
            }
            return result;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to list registrations", ex);
        }
    }

    private void addFilter(StringBuilder sql, List<String> args, Map<String, String> filters, String key, String column) {
        String value = filters.get(key);
        if (value != null && !value.trim().isEmpty()) {
            sql.append(" AND ").append(column).append("=?");
            args.add(value.trim());
        }
    }

    private ConsumerRegistration mapRegistration(ResultSet rs) throws Exception {
        return new ConsumerRegistration(
            rs.getString("id"),
            rs.getString("consumer"),
            rs.getString("topic"),
            rs.getString("environment"),
            rs.getString("owner_team"),
            RegistrationStatus.fromDbValue(rs.getString("status")),
            new HashMap<String, Object>(Jsons.asMap(rs.getString("config_json"))),
            rs.getLong("version"),
            rs.getString("created_at"),
            rs.getString("updated_at"),
            rs.getString("created_by"),
            rs.getString("updated_by")
        );
    }
}
