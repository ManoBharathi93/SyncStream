package com.syncstream.registry.db;

import com.syncstream.registry.model.ManagementAuditRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class ManagementAuditRepository {
    public void insert(Connection connection, ManagementAuditRecord record) {
        String sql = "INSERT INTO management_audit(id, action, target_type, target_id, before_json, after_json, actor, created_at) VALUES(?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, record.id());
            statement.setString(2, record.action());
            statement.setString(3, record.targetType());
            statement.setString(4, record.targetId());
            statement.setString(5, record.beforeJson());
            statement.setString(6, record.afterJson());
            statement.setString(7, record.actor());
            statement.setString(8, record.createdAt());
            statement.executeUpdate();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to insert management audit record", ex);
        }
    }

    public List<ManagementAuditRecord> list(Connection connection) {
        String sql = "SELECT * FROM management_audit ORDER BY created_at DESC LIMIT 100";
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            List<ManagementAuditRecord> records = new ArrayList<ManagementAuditRecord>();
            while (rs.next()) {
                records.add(new ManagementAuditRecord(
                    rs.getString("id"),
                    rs.getString("action"),
                    rs.getString("target_type"),
                    rs.getString("target_id"),
                    rs.getString("before_json"),
                    rs.getString("after_json"),
                    rs.getString("actor"),
                    rs.getString("created_at")
                ));
            }
            return records;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to list management audit records", ex);
        }
    }
}
