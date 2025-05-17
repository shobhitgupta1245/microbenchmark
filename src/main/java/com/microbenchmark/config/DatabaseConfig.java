package com.microbenchmark.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public interface DatabaseConfig {
    String getJdbcUrl();
    String getUsername();
    String getPassword();
    
    default Connection createConnection() throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
    }
} 