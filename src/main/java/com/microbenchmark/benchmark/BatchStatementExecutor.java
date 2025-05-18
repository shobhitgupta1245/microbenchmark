package com.microbenchmark.benchmark;

import com.microbenchmark.config.BenchmarkProfile;
import com.microbenchmark.config.DatabaseConfig;
import com.microbenchmark.metrics.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BatchStatementExecutor {
    private static final Logger logger = LoggerFactory.getLogger(BatchStatementExecutor.class);
    private final DatabaseConfig databaseConfig;
    private final BenchmarkProfile profile;
    private final MetricsService metricsService;

    public BatchStatementExecutor(DatabaseConfig databaseConfig, BenchmarkProfile profile, MetricsService metricsService) {
        this.databaseConfig = databaseConfig;
        this.profile = profile;
        this.metricsService = metricsService;
    }

    public void execute() {
        try (Connection conn = databaseConfig.createConnection()) {
            String insertUserSql = "INSERT INTO users (id, name, email, status, created_at, updated_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";
            String insertOrderSql = "INSERT INTO orders (id, user_id, order_status, total_amount, items_count, created_at, updated_at) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";

            int batchSize = profile.getBatchSize();
            int totalOperations = profile.getTotalOperations();
            Duration maxDuration = profile.getMaxDuration();
            Instant startTime = Instant.now();

            List<String> userIds = new ArrayList<>();
            try (PreparedStatement userStmt = conn.prepareStatement(insertUserSql);
                 PreparedStatement orderStmt = conn.prepareStatement(insertOrderSql)) {

                for (int i = 0; i < totalOperations && Duration.between(startTime, Instant.now()).compareTo(maxDuration) < 0; i++) {
                    String userId = UUID.randomUUID().toString();
                    userIds.add(userId);

                    // Insert user
                    userStmt.setString(1, userId);
                    userStmt.setString(2, "User " + i);
                    userStmt.setString(3, "user" + i + "@example.com");
                    userStmt.setString(4, "ACTIVE");
                    userStmt.addBatch();

                    // Insert order
                    orderStmt.setString(1, UUID.randomUUID().toString());
                    orderStmt.setString(2, userId);
                    orderStmt.setString(3, "PENDING");
                    orderStmt.setBigDecimal(4, BigDecimal.valueOf(100.0 + i).setScale(2, RoundingMode.HALF_UP));
                    orderStmt.setInt(5, i % 10 + 1);
                    orderStmt.addBatch();

                    if ((i + 1) % batchSize == 0) {
                        Instant batchStart = Instant.now();
                        userStmt.executeBatch();
                        orderStmt.executeBatch();
                        Duration batchDuration = Duration.between(batchStart, Instant.now());
                        metricsService.recordBatchExecution(batchSize, batchDuration);
                    }
                }

                // Execute any remaining statements
                userStmt.executeBatch();
                orderStmt.executeBatch();
            }
        } catch (SQLException e) {
            logger.error("Error executing batch statements", e);
            throw new RuntimeException("Batch execution failed", e);
        }
    }
} 