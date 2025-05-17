package com.microbenchmark.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class SpannerConfig implements DatabaseConfig {
    private final String projectId;
    private final String instanceId;
    private final String databaseId;
    private final String host;
    private final int port;

    public SpannerConfig(String projectId, String instanceId, String databaseId, String host, int port) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.databaseId = databaseId;
        this.host = host;
        this.port = port;
    }

    @Override
    public String getJdbcUrl() {
        // Use PostgreSQL JDBC URL format for pgAdapter with specific parameters
        return String.format("jdbc:postgresql://%s:%d/%s?sslmode=disable&preferQueryMode=simple",
            host, port, databaseId);
    }

    @Override
    public String getUsername() {
        return ""; // pgAdapter doesn't require username
    }

    @Override
    public String getPassword() {
        return ""; // pgAdapter doesn't require password
    }

    @Override
    public Connection createConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", getUsername());
        props.setProperty("password", getPassword());
        props.setProperty("sslmode", "disable");
        props.setProperty("preferQueryMode", "simple");
        props.setProperty("socketTimeout", "30");
        props.setProperty("connectTimeout", "30");
        
        return DriverManager.getConnection(getJdbcUrl(), props);
    }
} 