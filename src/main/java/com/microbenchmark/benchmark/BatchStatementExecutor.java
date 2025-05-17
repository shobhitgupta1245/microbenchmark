package com.microbenchmark.benchmark;

import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microbenchmark.config.DatabaseConfig;
import com.microbenchmark.metrics.MetricsService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class BatchStatementExecutor {
    private static final Logger logger = LoggerFactory.getLogger(BatchStatementExecutor.class);
    private final DatabaseConfig dbConfig;
    private final QueryProvider queryProvider;
    private final List<BenchmarkListener> listeners;
    private final AtomicLong operationsCompleted;
    private final MetricsService metricsService;
    private final Timer batchInsertTimer;

    public BatchStatementExecutor(DatabaseConfig dbConfig, QueryProvider queryProvider, String databaseType) {
        this.dbConfig = dbConfig;
        this.queryProvider = queryProvider;
        this.listeners = new ArrayList<>();
        this.operationsCompleted = new AtomicLong(0);
        this.metricsService = new MetricsService(databaseType);
        this.batchInsertTimer = metricsService.getQueryTimer("batch_insert");
    }

    public void addListener(BenchmarkListener listener) {
        listeners.add(listener);
    }

    public BenchmarkResult execute() {
        Instant start = Instant.now();
        long totalOperations = queryProvider.getTotalOperations();
        int batchSize = queryProvider.getBatchSize();
        long totalBatches = (totalOperations + batchSize - 1) / batchSize;

        try (Connection conn = dbConfig.createConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement stmt = conn.prepareStatement(queryProvider.getSql())) {
                for (long batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
                    Instant batchStart = Instant.now();
                    int statementsInBatch = 0;
                    try {
                        statementsInBatch = executeBatch(stmt, batchIndex, batchSize, totalOperations);
                        conn.commit();
                        Duration batchDuration = Duration.between(batchStart, Instant.now());
                        metricsService.recordBatchExecution(batchDuration, statementsInBatch);
                    } catch (SQLException e) {
                        conn.rollback();
                        metricsService.incrementFailedOperations();
                        throw e;
                    }
                    
                    // Notify progress
                    long completed = operationsCompleted.get();
                    double progress = (double) completed / totalOperations;
                    notifyProgress(progress, completed, totalOperations);
                }
            }
        } catch (SQLException e) {
            logger.error("Error executing batch statements", e);
            throw new RuntimeException("Batch execution failed", e);
        }

        Duration duration = Duration.between(start, Instant.now());
        metricsService.printMetrics();
        return new BenchmarkResult(totalOperations, duration);
    }

    private int executeBatch(PreparedStatement stmt, long batchIndex, int batchSize, long totalOperations) 
            throws SQLException {
        Timer.Sample batchSample = Timer.start();
        try {
            // For VariableBatchQueryProvider, this will add multiple statements to the batch
            queryProvider.setParameters(stmt, batchIndex);
            
            int[] results = stmt.executeBatch();
            long successCount = 0;
            for (int result : results) {
                if (result >= 0) successCount++;
            }
            operationsCompleted.addAndGet(successCount);
            metricsService.incrementTotalOperations(successCount);
            
            return results.length; // Return the actual number of statements in this batch
        } finally {
            batchSample.stop(batchInsertTimer);
        }
    }

    private void notifyProgress(double progress, long completed, long total) {
        for (BenchmarkListener listener : listeners) {
            listener.onProgress(progress, completed, total);
        }
    }

    public interface BenchmarkListener {
        void onProgress(double progress, long completed, long total);
    }

    public static class BenchmarkResult {
        private final long totalOperations;
        private final Duration duration;

        public BenchmarkResult(long totalOperations, Duration duration) {
            this.totalOperations = totalOperations;
            this.duration = duration;
        }

        public long getTotalOperations() {
            return totalOperations;
        }

        public Duration getDuration() {
            return duration;
        }

        public double getOperationsPerSecond() {
            return totalOperations / (duration.toMillis() / 1000.0);
        }
    }
} 