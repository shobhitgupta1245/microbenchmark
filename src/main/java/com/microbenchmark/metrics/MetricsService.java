package com.microbenchmark.metrics;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricsService {
    private static final Logger logger = LoggerFactory.getLogger(MetricsService.class);
    private final MeterRegistry registry;
    private final Counter totalOperations;
    private final Counter failedOperations;
    private final Timer.Builder timerBuilder;
    private final Map<Integer, Timer> batchSizeTimers;
    private final Map<Integer, Counter> batchSizeCounts;

    public MetricsService(String databaseType) {
        this.registry = new SimpleMeterRegistry();
        Tags tags = Tags.of("database", databaseType);

        this.totalOperations = Counter.builder("operations.total")
                .tags(tags)
                .description("Total number of operations processed")
                .register(registry);

        this.failedOperations = Counter.builder("operations.failed")
                .tags(tags)
                .description("Number of failed operations")
                .register(registry);

        this.timerBuilder = Timer.builder("query.execution")
                .tags(tags)
                .description("Time taken to execute individual queries")
                .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99);

        this.batchSizeTimers = new HashMap<>();
        this.batchSizeCounts = new HashMap<>();
    }

    public Timer getQueryTimer(String queryType) {
        return timerBuilder.tags("type", queryType).register(registry);
    }

    public void recordBatchExecution(Duration duration, int batchSize) {
        Timer timer = batchSizeTimers.computeIfAbsent(batchSize, size -> {
            Tags tags = Tags.of("database", "batch_size", String.valueOf(size));
            return Timer.builder("batch.execution")
                    .tags(tags)
                    .description("Time taken to execute a batch of specific size")
                    .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99)
                    .register(registry);
        });
        timer.record(duration);

        Counter counter = batchSizeCounts.computeIfAbsent(batchSize, size -> {
            Tags tags = Tags.of("database", "batch_size", String.valueOf(size));
            return Counter.builder("batch.count")
                    .tags(tags)
                    .description("Number of batches of specific size")
                    .register(registry);
        });
        counter.increment();
    }

    public void incrementTotalOperations(long count) {
        totalOperations.increment(count);
    }

    public void incrementFailedOperations() {
        failedOperations.increment();
    }

    public void printMetrics() {
        logger.info("\nMetrics Summary:");
        logger.info("---------------");
        
        // Print metrics for each batch size
        batchSizeTimers.forEach((batchSize, timer) -> {
            Counter counter = batchSizeCounts.get(batchSize);
            double batchCount = counter.count();
            
            logger.info("\nBatch Size: {} statements", batchSize);
            logger.info("Number of batches: {}", (long)batchCount);
            logger.info("Execution Times:");
            logger.info("  P50: {:.2f} ms", timer.percentile(0.5, TimeUnit.MILLISECONDS));
            logger.info("  P75: {:.2f} ms", timer.percentile(0.75, TimeUnit.MILLISECONDS));
            logger.info("  P90: {:.2f} ms", timer.percentile(0.90, TimeUnit.MILLISECONDS));
            logger.info("  P95: {:.2f} ms", timer.percentile(0.95, TimeUnit.MILLISECONDS));
            logger.info("  P99: {:.2f} ms", timer.percentile(0.99, TimeUnit.MILLISECONDS));
            logger.info("  Mean: {:.2f} ms", timer.mean(TimeUnit.MILLISECONDS));
            logger.info("  Max: {:.2f} ms", timer.max(TimeUnit.MILLISECONDS));
            
            double totalTime = timer.totalTime(TimeUnit.SECONDS);
            double statementsPerSecond = (batchCount * batchSize) / totalTime;
            logger.info("  Throughput: {:.2f} statements/second", statementsPerSecond);
        });
        
        // Overall metrics
        logger.info("\nOverall Statistics:");
        logger.info("Total Operations: {}", totalOperations.count());
        logger.info("Failed Operations: {}", failedOperations.count());
        
        // Calculate overall throughput
        double totalStatements = batchSizeCounts.entrySet().stream()
                .mapToDouble(e -> e.getKey() * e.getValue().count())
                .sum();
        double totalTime = batchSizeTimers.values().stream()
                .mapToDouble(t -> t.totalTime(TimeUnit.SECONDS))
                .max()
                .orElse(0.0);
        logger.info("Total Time: {:.2f} seconds", totalTime);
        logger.info("Overall Throughput: {:.2f} statements/second", totalStatements / totalTime);
    }
} 