package com.shobhitsg.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SpannerConfig implements DatabaseConfig {
    private final String projectId;
    private final String instanceId;
    private final String databaseId;

    public SpannerConfig(String projectId, String instanceId, String databaseId) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.databaseId = databaseId;
    }

    @Override
    public Connection createConnection() throws SQLException {
        String jdbcUrl = String.format("jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s",
                projectId, instanceId, databaseId);
        return DriverManager.getConnection(jdbcUrl);
    }
} 