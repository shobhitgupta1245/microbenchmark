package com.microbenchmark.metrics;

import com.microbenchmark.config.MonitoringConfig;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.stackdriver.StackdriverConfig;
import io.micrometer.stackdriver.StackdriverMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class MetricsService {
    private static final Logger logger = LoggerFactory.getLogger(MetricsService.class);
    private final MonitoringConfig config;
    private final MeterRegistry registry;
    private final Map<Integer, Timer> batchTimers;
    private final MeterRegistry cloudRegistry;
    private final Counter totalOperations;
    private final Counter failedOperations;
    private final Timer.Builder timerBuilder;
    private final Map<Integer, Counter> batchSizeCounts;
    private final String databaseType;
    private final boolean enableCloudMetrics;

    public MetricsService(MonitoringConfig config, String databaseType, String projectId) {
        this.config = config;
        this.databaseType = databaseType;
        this.enableCloudMetrics = projectId != null && !projectId.isEmpty();

        // Local registry for immediate feedback
        this.registry = new SimpleMeterRegistry();
        
        // Cloud registry for Google Cloud Monitoring
        if (enableCloudMetrics) {
            this.cloudRegistry = createCloudRegistry(projectId);
            logger.info("Google Cloud Monitoring enabled for project: {}", projectId);
        } else {
            this.cloudRegistry = null;
            logger.info("Google Cloud Monitoring disabled");
        }

        Tags tags = Tags.of(
            "database", databaseType,
            "application", "microbenchmark"
        );

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

        this.batchTimers = new HashMap<>();
        this.batchSizeCounts = new HashMap<>();

        // Register with cloud if enabled
        if (enableCloudMetrics) {
            Counter.builder("operations.total")
                  .tags(tags)
                  .description("Total number of operations processed")
                  .register(cloudRegistry);

            Counter.builder("operations.failed")
                  .tags(tags)
                  .description("Number of failed operations")
                  .register(cloudRegistry);
        }
    }

    private MeterRegistry createCloudRegistry(String projectId) {
        try {
            StackdriverConfig config = new StackdriverConfig() {
                @Override
                public String get(String key) {
                    return null;
                }

                @Override
                public String projectId() {
                    return projectId;
                }

                @Override
                public Duration step() {
                    return Duration.ofSeconds(60); // Report metrics every minute
                }
            };

            return StackdriverMeterRegistry.builder(config).build();
        } catch (Exception e) {
            logger.warn("Failed to create cloud registry. Using local registry only: {}", e.getMessage());
            return new SimpleMeterRegistry();
        }
    }

    public Timer getQueryTimer(String queryType) {
        Timer localTimer = timerBuilder.tags("type", queryType).register(registry);
        
        if (enableCloudMetrics) {
            Timer cloudTimer = timerBuilder.tags("type", queryType).register(cloudRegistry);
            return new Timer() {
                @Override
                public void record(long amount, TimeUnit unit) {
                    localTimer.record(amount, unit);
                    cloudTimer.record(amount, unit);
                }

                @Override
                public void record(Duration duration) {
                    localTimer.record(duration);
                    cloudTimer.record(duration);
                }

                @Override
                public <T> T record(Supplier<T> supplier) {
                    return localTimer.record(supplier);
                }

                @Override
                public <T> T recordCallable(Callable<T> callable) throws Exception {
                    return localTimer.recordCallable(callable);
                }

                @Override
                public void record(Runnable runnable) {
                    localTimer.record(runnable);
                }

                @Override
                public long count() {
                    return localTimer.count();
                }

                @Override
                public double totalTime(TimeUnit unit) {
                    return localTimer.totalTime(unit);
                }

                @Override
                public double max(TimeUnit unit) {
                    return localTimer.max(unit);
                }

                @Override
                public HistogramSnapshot takeSnapshot() {
                    return localTimer.takeSnapshot();
                }

                @Override
                public TimeUnit baseTimeUnit() {
                    return localTimer.baseTimeUnit();
                }

                @Override
                public Meter.Id getId() {
                    return localTimer.getId();
                }

                @Override
                public void close() {
                    localTimer.close();
                    cloudTimer.close();
                }
            };
        }
        
        return localTimer;
    }

    public void recordBatchExecution(int batchSize, Duration duration) {
        Timer timer = batchTimers.computeIfAbsent(batchSize,
            size -> Timer.builder("batch.execution")
                .tag("batch.size", String.valueOf(size))
                .register(registry));
        timer.record(duration);

        Counter counter = batchSizeCounts.computeIfAbsent(batchSize, size -> {
            Tags tags = Tags.of(
                "database", databaseType,
                "batch_size", String.valueOf(size),
                "application", "microbenchmark"
            );

            Counter localCounter = Counter.builder("batch.count")
                    .tags(tags)
                    .description("Number of batches of specific size")
                    .register(registry);

            if (enableCloudMetrics) {
                Counter.builder("batch.count")
                      .tags(tags)
                      .description("Number of batches of specific size")
                      .register(cloudRegistry);
            }

            return localCounter;
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
        logger.info("Google Cloud Monitoring {}", config.isCloudMonitoringEnabled() ? "enabled" : "disabled");
        if (batchTimers.isEmpty()) {
            return;
        }

        logger.info("\nMetrics Summary:");
        logger.info("---------------");

        for (Map.Entry<Integer, Timer> entry : batchTimers.entrySet()) {
            int batchSize = entry.getKey();
            Timer timer = entry.getValue();
            HistogramSnapshot snapshot = timer.takeSnapshot();

            logger.info("\nBatch Size: {} statements", batchSize);
            logger.info("Number of batches: {}", timer.count());
            logger.info("Execution Times:");
            logger.info("  P50: {:.2f} ms", snapshot.percentileValues()[0].value());
            logger.info("  P75: {:.2f} ms", snapshot.percentileValues()[1].value());
            logger.info("  P90: {:.2f} ms", snapshot.percentileValues()[2].value());
            logger.info("  P95: {:.2f} ms", snapshot.percentileValues()[3].value());
            logger.info("  P99: {:.2f} ms", snapshot.percentileValues()[4].value());
            logger.info("  Mean: {:.2f} ms", snapshot.mean());
            logger.info("  Max: {:.2f} ms", snapshot.max());
            logger.info("  Throughput: {:.2f} statements/second", calculateThroughput(timer, batchSize));
        }

        logger.info("\nOverall Statistics:");
        double totalOperations = batchTimers.entrySet().stream()
            .mapToDouble(e -> e.getKey() * e.getValue().count())
            .sum();
        logger.info("Total Operations: {}", totalOperations);
        logger.info("Failed Operations: {}", 0.0); // We're not tracking failures yet
        logger.info("Total Time: {:.2f} seconds", getTotalTimeSeconds());
        logger.info("Overall Throughput: {:.2f} statements/second", totalOperations / getTotalTimeSeconds());
    }

    private double calculateThroughput(Timer timer, int batchSize) {
        return (timer.count() * batchSize) / getTotalTimeSeconds();
    }

    private double getTotalTimeSeconds() {
        return batchTimers.values().stream()
            .mapToDouble(timer -> timer.totalTime(TimeUnit.SECONDS))
            .sum();
    }

    public void close() {
        if (enableCloudMetrics && cloudRegistry instanceof AutoCloseable) {
            try {
                ((AutoCloseable) cloudRegistry).close();
            } catch (Exception e) {
                logger.error("Error closing cloud registry", e);
            }
        }
    }
} 