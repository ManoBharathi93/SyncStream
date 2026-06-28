package com.syncstream.registry.db;

import com.syncstream.registry.model.RegistrationAuditRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AuditRepository {
    public void insert(Connection connection, RegistrationAuditRecord record) {
        String sql = "INSERT INTO registration_audit(id, registration_id, action, before_json, after_json, actor, created_at) VALUES(?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, record.id());
            statement.setString(2, record.registrationId());
            statement.setString(3, record.action());
            statement.setString(4, record.beforeJson());
            statement.setString(5, record.afterJson());
            statement.setString(6, record.actor());
            statement.setString(7, record.createdAt());
            statement.executeUpdate();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to insert audit record", ex);
        }
    }

    public List<RegistrationAuditRecord> listByRegistrationId(Connection connection, String registrationId) {
        String sql = "SELECT * FROM registration_audit WHERE registration_id=? ORDER BY created_at ASC";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, registrationId);
            List<RegistrationAuditRecord> result = new ArrayList<RegistrationAuditRecord>();
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    result.add(new RegistrationAuditRecord(
                        rs.getString("id"),
                        rs.getString("registration_id"),
                        rs.getString("action"),
                        rs.getString("before_json"),
                        rs.getString("after_json"),
                        rs.getString("actor"),
                        rs.getString("created_at")
                    ));
                }
            }
            return result;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to list audit records", ex);
        }
    }
}
