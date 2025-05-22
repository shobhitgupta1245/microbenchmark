package com.microbenchmark.benchmark;

import com.microbenchmark.config.BenchmarkProfile;
import com.microbenchmark.config.DatabaseConfig;
import com.microbenchmark.metrics.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

public class BatchStatementExecutor {
    private static final Logger logger = LoggerFactory.getLogger(BatchStatementExecutor.class);
    private final DatabaseConfig databaseConfig;
    private final BenchmarkProfile profile;
    private final MetricsService metricsService;
    private final QueryProvider queryProvider;

    public BatchStatementExecutor(DatabaseConfig databaseConfig, BenchmarkProfile profile, 
                                MetricsService metricsService, QueryProvider queryProvider) {
        this.databaseConfig = databaseConfig;
        this.profile = profile;
        this.metricsService = metricsService;
        this.queryProvider = queryProvider;
    }

    public void execute() {
        try (Connection conn = databaseConfig.createConnection()) {
            String sql = queryProvider.getSql();
            int batchSize = queryProvider.getBatchSize();
            long totalOperations = queryProvider.getTotalOperations();
            Duration maxDuration = profile.getMaxDuration();
            Instant startTime = Instant.now();

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (long i = 0; i < totalOperations && Duration.between(startTime, Instant.now()).compareTo(maxDuration) < 0; i++) {
                    queryProvider.setParameters(stmt, i);

                    if ((i + 1) % batchSize == 0) {
                        Instant batchStart = Instant.now();
                        stmt.executeBatch();
                        Duration batchDuration = Duration.between(batchStart, Instant.now());
                        metricsService.recordBatchExecution(batchSize, batchDuration);
                    }
                }

                // Execute any remaining statements
                stmt.executeBatch();
            }
        } catch (SQLException e) {
            logger.error("Error executing batch statements", e);
            throw new RuntimeException("Batch execution failed", e);
        }
    }
} 