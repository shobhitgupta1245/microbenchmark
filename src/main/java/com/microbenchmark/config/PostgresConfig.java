package com.microbenchmark.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class PostgresConfig implements DatabaseConfig {
    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;

    public PostgresConfig(String host, int port, String database, String username, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    @Override
    public Connection createConnection() throws SQLException {
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("reWriteBatchedStatements", "false");
        return DriverManager.getConnection(jdbcUrl, props);
    }
} 