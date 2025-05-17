package com.microbenchmark.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresConfig implements DatabaseConfig {
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public PostgresConfig(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public Connection createConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }
} 