package com.microbenchmark.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class SpannerConfig implements DatabaseConfig {
    private final String projectId;
    private final String instanceId;
    private final String databaseId;
    private final SpannerConnectionType connectionType;

    public SpannerConfig(String projectId, String instanceId, String databaseId, SpannerConnectionType connectionType) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.databaseId = databaseId;
        this.connectionType = connectionType;
    }

    @Override
    public Connection createConnection() throws SQLException {
        String jdbcUrl;
        Properties props = new Properties();

        if (connectionType == SpannerConnectionType.JDBC_DIRECT) {
            jdbcUrl = String.format("jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s",
                projectId, instanceId, databaseId);
            
            // Set emulator-specific properties
            // props.setProperty("usePlainText", "true");
            // props.setProperty("autoConfigEmulator", "true");
            props.setProperty("minSessions", "1");
            props.setProperty("maxSessions", "4");
            props.setProperty("credentials", "/Users/shobhitgup/Downloads/span-cloud-testing.json");
        } else {
            // PGAdapter connection
            jdbcUrl = String.format("jdbc:postgresql://localhost:5432/%s?options=-c%%20spanner.project_id=%s%%20-c%%20spanner.instance_id=%s%%20-c%%20spanner.database_id=%s",
                databaseId, projectId, instanceId, databaseId);
            
            // Set PGAdapter-specific properties
            props.setProperty("user", "postgres");
            props.setProperty("password", "");
            props.setProperty("socketTimeout", "30");
            props.setProperty("tcpKeepAlive", "true");
            props.setProperty("ssl", "false");
            props.setProperty("sslmode", "disable");
            props.setProperty("reWriteBatchedInserts", "true");
            props.setProperty("prepareThreshold", "1");
            props.setProperty("preparedStatementCacheQueries", "0");
            props.setProperty("autosave", "never");
            props.setProperty("binaryTransfer", "true");
            props.setProperty("loginTimeout", "10");
            props.setProperty("ApplicationName", "microbenchmark");
            props.setProperty("preferQueryMode", "simple");
        }

        return DriverManager.getConnection(jdbcUrl, props);
    }

    @Override
    public String getJdbcUrl() {
        if (connectionType == SpannerConnectionType.JDBC_DIRECT) {
            return String.format("jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s",
                projectId, instanceId, databaseId);
        } else {
            return String.format("jdbc:postgresql://localhost:5432/%s?options=-c%%20spanner.project_id=%s%%20-c%%20spanner.instance_id=%s%%20-c%%20spanner.database_id=%s",
                databaseId, projectId, instanceId, databaseId);
        }
    }

    @Override
    public String getUsername() {
        return connectionType == SpannerConnectionType.PGADAPTER_JDBC ? "postgres" : null;
    }

    @Override
    public String getPassword() {
        return connectionType == SpannerConnectionType.PGADAPTER_JDBC ? "" : null;
    }

    public SpannerConnectionType getConnectionType() {
        return connectionType;
    }
} 