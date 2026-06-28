package com.syncstream.registry.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseManager {
    private final String jdbcUrl;

    public DatabaseManager(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public Connection connection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    public void initialize() {
        try (Connection connection = connection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)");
        } catch (SQLException ex) {
            throw new IllegalStateException("Failed to initialize schema_migrations table", ex);
        }
    }
}
