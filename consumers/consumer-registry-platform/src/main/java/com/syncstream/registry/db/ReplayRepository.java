package com.syncstream.registry.db;

import com.syncstream.registry.model.ReplayRequestRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class ReplayRepository {
    public void insert(Connection connection, ReplayRequestRecord record) {
        String sql = "INSERT INTO replay_requests(id, topic, consumer_group, start_timestamp, start_offset, end_offset, reason, status, created_at, created_by, updated_at, updated_by) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, record.id());
            statement.setString(2, record.topic());
            statement.setString(3, record.consumerGroup());
            statement.setString(4, record.startTimestamp());
            statement.setString(5, record.startOffset());
            statement.setString(6, record.endOffset());
            statement.setString(7, record.reason());
            statement.setString(8, record.status());
            statement.setString(9, record.createdAt());
            statement.setString(10, record.createdBy());
            statement.setString(11, record.updatedAt());
            statement.setString(12, record.updatedBy());
            statement.executeUpdate();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to insert replay request", ex);
        }
    }

    public List<ReplayRequestRecord> list(Connection connection) {
        String sql = "SELECT * FROM replay_requests ORDER BY created_at DESC";
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rs = statement.executeQuery()) {
            List<ReplayRequestRecord> records = new ArrayList<ReplayRequestRecord>();
            while (rs.next()) {
                records.add(new ReplayRequestRecord(
                    rs.getString("id"),
                    rs.getString("topic"),
                    rs.getString("consumer_group"),
                    rs.getString("start_timestamp"),
                    rs.getString("start_offset"),
                    rs.getString("end_offset"),
                    rs.getString("reason"),
                    rs.getString("status"),
                    rs.getString("created_at"),
                    rs.getString("created_by"),
                    rs.getString("updated_at"),
                    rs.getString("updated_by")
                ));
            }
            return records;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to list replay requests", ex);
        }
    }
}
