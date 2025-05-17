package com.shobhitsg.benchmark;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.Random;
import java.util.UUID;

public class ComplexQueryProvider implements QueryProvider {
    private static final int MIN_STATEMENTS = 5;
    private static final int MAX_STATEMENTS = 15;
    private static final int BATCH_SIZE = 1000;
    private static final long TOTAL_OPERATIONS = 1_000_000;
    
    private static final String[] USER_STATUSES = {"ACTIVE", "INACTIVE", "SUSPENDED"};
    private static final String[] ORDER_STATUSES = {"NEW", "PROCESSING", "COMPLETED", "CANCELLED"};
    
    private final Random random;
    private final OperationType operationType;

    public enum OperationType {
        USER_INSERT,
        ORDER_INSERT,
        USER_UPDATE,
        ORDER_UPDATE,
        MIXED
    }

    public ComplexQueryProvider(OperationType operationType) {
        this.random = new Random();
        this.operationType = operationType;
    }

    @Override
    public String getSql() {
        return switch (operationType) {
            case USER_INSERT -> "INSERT INTO users (id, name, email, status, created_at, updated_at) " +
                              "VALUES (?, ?, ?, ?, ?, ?)";
            case ORDER_INSERT -> "INSERT INTO orders (id, user_id, order_status, total_amount, items_count, created_at, updated_at) " +
                               "VALUES (?, ?, ?, ?, ?, ?, ?)";
            case USER_UPDATE -> "UPDATE users SET status = ?, updated_at = ? WHERE id = ?";
            case ORDER_UPDATE -> "UPDATE orders SET order_status = ?, total_amount = ?, items_count = ?, updated_at = ? WHERE id = ?";
            case MIXED -> throw new IllegalStateException("MIXED type requires multiple statements and cannot provide a single SQL");
        };
    }

    @Override
    public void setParameters(PreparedStatement stmt, long batchIndex) throws SQLException {
        int statementsInBatch = random.nextInt(MIN_STATEMENTS, MAX_STATEMENTS + 1);
        
        if (operationType == OperationType.MIXED) {
            executeMixedBatch(stmt, batchIndex, statementsInBatch);
        } else {
            for (int i = 0; i < statementsInBatch; i++) {
                setParametersForType(stmt, batchIndex, i);
                stmt.addBatch();
            }
        }
    }

    private void executeMixedBatch(PreparedStatement stmt, long batchIndex, int statementsInBatch) throws SQLException {
        // For MIXED type, we'll need to prepare and execute different types of statements
        try (PreparedStatement userInsertStmt = stmt.getConnection().prepareStatement(getSqlForType(OperationType.USER_INSERT));
             PreparedStatement orderInsertStmt = stmt.getConnection().prepareStatement(getSqlForType(OperationType.ORDER_INSERT));
             PreparedStatement userUpdateStmt = stmt.getConnection().prepareStatement(getSqlForType(OperationType.USER_UPDATE));
             PreparedStatement orderUpdateStmt = stmt.getConnection().prepareStatement(getSqlForType(OperationType.ORDER_UPDATE))) {
            
            for (int i = 0; i < statementsInBatch; i++) {
                // Randomly choose operation type
                OperationType type = OperationType.values()[random.nextInt(4)]; // Excluding MIXED
                PreparedStatement currentStmt = switch (type) {
                    case USER_INSERT -> userInsertStmt;
                    case ORDER_INSERT -> orderInsertStmt;
                    case USER_UPDATE -> userUpdateStmt;
                    case ORDER_UPDATE -> orderUpdateStmt;
                    default -> throw new IllegalStateException("Unexpected value: " + type);
                };
                
                setParametersForType(currentStmt, batchIndex, i);
                currentStmt.addBatch();
            }
            
            // Execute all batches
            userInsertStmt.executeBatch();
            orderInsertStmt.executeBatch();
            userUpdateStmt.executeBatch();
            orderUpdateStmt.executeBatch();
        }
    }

    private String getSqlForType(OperationType type) {
        return new ComplexQueryProvider(type).getSql();
    }

    private void setParametersForType(PreparedStatement stmt, long batchIndex, int index) throws SQLException {
        Timestamp now = Timestamp.from(Instant.now());
        String id = UUID.randomUUID().toString();
        
        switch (operationType) {
            case USER_INSERT -> {
                stmt.setString(1, id);
                stmt.setString(2, "User" + batchIndex + "_" + index);
                stmt.setString(3, "user" + batchIndex + "_" + index + "@example.com");
                stmt.setString(4, USER_STATUSES[random.nextInt(USER_STATUSES.length)]);
                stmt.setTimestamp(5, now);
                stmt.setTimestamp(6, now);
            }
            case ORDER_INSERT -> {
                stmt.setString(1, id);
                stmt.setString(2, UUID.randomUUID().toString()); // random user_id
                stmt.setString(3, ORDER_STATUSES[random.nextInt(ORDER_STATUSES.length)]);
                stmt.setDouble(4, random.nextDouble() * 1000.0); // random amount up to 1000
                stmt.setInt(5, random.nextInt(1, 11)); // 1-10 items
                stmt.setTimestamp(6, now);
                stmt.setTimestamp(7, now);
            }
            case USER_UPDATE -> {
                stmt.setString(1, USER_STATUSES[random.nextInt(USER_STATUSES.length)]);
                stmt.setTimestamp(2, now);
                stmt.setString(3, UUID.randomUUID().toString());
            }
            case ORDER_UPDATE -> {
                stmt.setString(1, ORDER_STATUSES[random.nextInt(ORDER_STATUSES.length)]);
                stmt.setDouble(2, random.nextDouble() * 1000.0);
                stmt.setInt(3, random.nextInt(1, 11));
                stmt.setTimestamp(4, now);
                stmt.setString(5, UUID.randomUUID().toString());
            }
            default -> throw new IllegalStateException("Unexpected operation type: " + operationType);
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

    public OperationType getOperationType() {
        return operationType;
    }
} 