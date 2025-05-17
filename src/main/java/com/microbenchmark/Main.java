package com.microbenchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microbenchmark.benchmark.BatchStatementExecutor;
import com.microbenchmark.benchmark.ComplexQueryProvider;
import com.microbenchmark.benchmark.QueryProvider;
import com.microbenchmark.config.ConfigurationLoader;
import com.microbenchmark.config.DatabaseConfig;

import java.time.Duration;
import java.util.Arrays;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // Load configurations from properties files
        DatabaseConfig spannerConfig = ConfigurationLoader.loadSpannerConfig();
        DatabaseConfig postgresConfig = ConfigurationLoader.loadPostgresConfig();

        // Run benchmarks for each operation type
        Arrays.stream(ComplexQueryProvider.OperationType.values())
              .forEach(opType -> {
                  logger.info("\n=== Starting benchmark for operation type: {} ===\n", opType);
                  
                  // Create query provider for this operation type
                  QueryProvider queryProvider = new ComplexQueryProvider(opType);

                  // Run Spanner benchmark
                  logger.info("Starting Spanner benchmark...");
                  runBenchmark("Spanner-" + opType, spannerConfig, queryProvider);

                  // Run PostgreSQL benchmark
                  logger.info("Starting PostgreSQL benchmark...");
                  runBenchmark("PostgreSQL-" + opType, postgresConfig, queryProvider);
                  
                  logger.info("\n=== Completed benchmark for operation type: {} ===\n", opType);
              });
    }

    private static void runBenchmark(String name, DatabaseConfig dbConfig, QueryProvider queryProvider) {
        BatchStatementExecutor executor = new BatchStatementExecutor(dbConfig, queryProvider, name);
        
        // Add progress listener
        executor.addListener((progress, completed, total) -> {
            if (completed % 100000 == 0 || completed == total) {
                logger.info("{}: Completed {} of {} operations ({:.2f}%)",
                        name, completed, total, progress * 100);
            }
        });

        try {
            BatchStatementExecutor.BenchmarkResult result = executor.execute();
            
            Duration duration = result.getDuration();
            long operations = result.getTotalOperations();
            double opsPerSecond = result.getOperationsPerSecond();

            logger.info("");
            logger.info("{} Benchmark Results:", name);
            logger.info("Total Operations: {}", operations);
            logger.info("Total Duration: {} seconds", duration.toSeconds());
            logger.info("Operations per Second: {:.2f}", opsPerSecond);
            logger.info("");
        } catch (Exception e) {
            logger.error("{} benchmark failed", name, e);
        }
    }
}