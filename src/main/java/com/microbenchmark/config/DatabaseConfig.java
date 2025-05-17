package com.microbenchmark.config;

import java.sql.Connection;
import java.sql.SQLException;

public interface DatabaseConfig {
    Connection createConnection() throws SQLException;
} 