package com.microbenchmark.benchmark;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.UUID;

public class VariableBatchQueryProvider implements QueryProvider {
    private static final int MIN_STATEMENTS = 5;
    private static final int MAX_STATEMENTS = 15;
    private static final int BATCH_SIZE = 1000;  // Number of batches to process
    private static final long TOTAL_OPERATIONS = 1_000_000;
    private final Random random;

    public VariableBatchQueryProvider() {
        this.random = new Random();
    }

    @Override
    public String getSql() {
        return "INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)";
    }

    @Override
    public void setParameters(PreparedStatement stmt, long batchIndex) throws SQLException {
        // Generate a random number of statements for this batch
        int statementsInBatch = random.nextInt(MIN_STATEMENTS, MAX_STATEMENTS + 1);
        
        // Execute multiple statements for this batch
        for (int i = 0; i < statementsInBatch; i++) {
            String id = UUID.randomUUID().toString();
            String name = String.format("User%d_%d", batchIndex, i);
            String email = String.format("user%d_%d@example.com", batchIndex, i);
            
            stmt.setString(1, id);
            stmt.setString(2, name);
            stmt.setString(3, email);
            stmt.addBatch();
        }
    }

    @Override
    public int getBatchSize() {
        return BATCH_SIZE;
    }

    @Override
    public long getTotalOperations() {
        return TOTAL_OPERATIONS;
    }

    public int getMinStatements() {
        return MIN_STATEMENTS;
    }

    public int getMaxStatements() {
        return MAX_STATEMENTS;
    }
} 