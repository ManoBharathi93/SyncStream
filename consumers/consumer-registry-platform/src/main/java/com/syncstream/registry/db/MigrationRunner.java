package com.syncstream.registry.db;

import com.syncstream.registry.util.Times;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MigrationRunner {
    private final DatabaseManager databaseManager;

    public MigrationRunner(DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
    }

    public void migrate() {
        List<String> versions = Arrays.asList("V1", "V2");
        for (String version : versions) {
            if (!isApplied(version)) {
                String sql = loadMigrationSql(version);
                apply(version, sql);
            }
        }
    }

    private String loadMigrationSql(String version) {
        if ("V1".equals(version)) {
            return loadResource("db/migrations/V1__create_registry_schema.sql");
        }
        if ("V2".equals(version)) {
            return loadResource("db/migrations/V2__create_dashboard_schema.sql");
        }
        throw new IllegalArgumentException("Unsupported migration version: " + version);
    }

    private boolean isApplied(String version) {
        try (Connection connection = databaseManager.connection();
             PreparedStatement statement = connection.prepareStatement("SELECT 1 FROM schema_migrations WHERE version = ?")) {
            statement.setString(1, version);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next();
            }
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to check schema migration state", ex);
        }
    }

    private void apply(String version, String sql) {
        try (Connection connection = databaseManager.connection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                for (String chunk : splitStatements(sql)) {
                    if (!chunk.trim().isEmpty()) {
                        statement.execute(chunk);
                    }
                }
            }

            try (PreparedStatement migrationInsert = connection.prepareStatement(
                "INSERT INTO schema_migrations(version, applied_at) VALUES(?, ?)")) {
                migrationInsert.setString(1, version);
                migrationInsert.setString(2, Times.nowIsoUtc());
                migrationInsert.executeUpdate();
            }

            connection.commit();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to apply migration " + version, ex);
        }
    }

    private List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<String>();
        for (String segment : sql.split(";")) {
            String trimmed = segment.trim();
            if (!trimmed.isEmpty()) {
                statements.add(trimmed);
            }
        }
        return statements;
    }

    private String loadResource(String path) {
        InputStream stream = MigrationRunner.class.getClassLoader().getResourceAsStream(path);
        if (stream == null) {
            throw new IllegalStateException("Missing migration resource: " + path);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append('\n');
            }
            return builder.toString();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to load migration resource " + path, ex);
        }
    }
}
