package com.microbenchmark;

import com.microbenchmark.config.BenchmarkProfile;
import com.microbenchmark.config.ConfigurationLoader;
import com.microbenchmark.config.DatabaseConfig;
import com.microbenchmark.config.MonitoringConfig;
import com.microbenchmark.metrics.MetricsService;
import com.microbenchmark.benchmark.BatchStatementExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            logger.error("Please provide a profile (spanner or postgres)");
            System.exit(1);
        }

        String profileName = args[0].toLowerCase();
        Properties config = ConfigurationLoader.loadConfig();
        DatabaseConfig dbConfig;
        
        try {
            switch (profileName) {
                case "spanner":
                    dbConfig = ConfigurationLoader.createSpannerConfig(config);
                    break;
                case "postgres":
                    dbConfig = ConfigurationLoader.createPostgresConfig(config);
                    break;
                default:
                    logger.error("Invalid profile: {}. Valid profiles are: spanner, postgres", profileName);
                    System.exit(1);
                    return;
            }

            // Create monitoring configuration
            MonitoringConfig monitoringConfig = new MonitoringConfig(
                Boolean.parseBoolean(config.getProperty("metrics.enabled", "true")),
                config.getProperty("google.cloud.project.id"),
                config.getProperty("metrics.prefix", "")
            );

            // Create metrics service
            MetricsService metricsService = new MetricsService(
                monitoringConfig,
                profileName,  // Use the profile name as database type
                config.getProperty("google.cloud.project.id")
            );

            // Create benchmark profile
            BenchmarkProfile benchmarkProfile = new BenchmarkProfile(
                Integer.parseInt(config.getProperty("batch.size", "100")),
                Integer.parseInt(config.getProperty("total.operations", "10000")),
                Duration.ofMinutes(Integer.parseInt(config.getProperty("duration.minutes", "5")))
            );

            // Create and run executor
            BatchStatementExecutor executor = new BatchStatementExecutor(
                dbConfig,
                benchmarkProfile,
                metricsService
            );

            executor.execute();

        } catch (Exception e) {
            logger.error("Error running benchmark", e);
            System.exit(1);
        }
    }
}