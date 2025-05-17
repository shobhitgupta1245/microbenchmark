package com.microbenchmark.benchmark;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

public class UserInsertQueryProvider implements QueryProvider {
    private static final int BATCH_SIZE = 1000;
    private static final long TOTAL_OPERATIONS = 1_000_000;

    @Override
    public String getSql() {
        return "INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)";
    }

    @Override
    public void setParameters(PreparedStatement stmt, long batchIndex) throws SQLException {
        String id = UUID.randomUUID().toString();
        String name = "User" + batchIndex;
        String email = "user" + batchIndex + "@example.com";
        
        stmt.setString(1, id);
        stmt.setString(2, name);
        stmt.setString(3, email);
    }

    @Override
    public int getBatchSize() {
        return BATCH_SIZE;
    }

    @Override
    public long getTotalOperations() {
        return TOTAL_OPERATIONS;
    }
} 