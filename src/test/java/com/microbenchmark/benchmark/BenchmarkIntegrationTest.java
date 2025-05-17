package com.microbenchmark.benchmark;

import com.microbenchmark.config.BenchmarkProfile;
import com.microbenchmark.config.DatabaseConfig;
import com.microbenchmark.config.MonitoringConfig;
import com.microbenchmark.config.PostgresConfig;
import com.microbenchmark.config.SpannerConfig;
import com.microbenchmark.metrics.MetricsService;
import com.microbenchmark.test.EmulatorTestHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BenchmarkIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkIntegrationTest.class);
    private EmulatorTestHelper emulatorHelper;
    private Properties testConfig;

    @BeforeAll
    void setUp() {
        emulatorHelper = new EmulatorTestHelper();
        testConfig = emulatorHelper.getTestConfig();
        emulatorHelper.startPostgres();
        emulatorHelper.startSpannerEmulator();
    }

    @AfterAll
    void tearDown() {
        emulatorHelper.stopAll();
    }

    @Test
    void testPostgresBenchmark() {
        DatabaseConfig postgresConfig = new PostgresConfig(
            emulatorHelper.getPostgresJdbcUrl(),
            emulatorHelper.getPostgresUsername(),
            emulatorHelper.getPostgresPassword()
        );

        MonitoringConfig monitoringConfig = new MonitoringConfig(false, null, null);
        MetricsService metricsService = new MetricsService(
            monitoringConfig,
            "postgres",
            testConfig.getProperty("spanner.emulator.project")
        );

        BenchmarkProfile profile = new BenchmarkProfile(
            Integer.parseInt(testConfig.getProperty("test.batch.size")),
            Integer.parseInt(testConfig.getProperty("test.total.operations")),
            Duration.ofMinutes(Integer.parseInt(testConfig.getProperty("test.duration.minutes")))
        );

        BatchStatementExecutor executor = new BatchStatementExecutor(
            postgresConfig,
            profile,
            metricsService
        );

        assertDoesNotThrow(() -> executor.execute());
    }

    @Test
    void testSpannerBenchmark() {
        DatabaseConfig spannerConfig = new SpannerConfig(
            testConfig.getProperty("spanner.emulator.project"),
            testConfig.getProperty("spanner.emulator.instance"),
            testConfig.getProperty("spanner.emulator.database"),
            emulatorHelper.getSpannerEmulatorHost(),
            emulatorHelper.getPgAdapterPort()
        );

        MonitoringConfig monitoringConfig = new MonitoringConfig(false, null, null);
        MetricsService metricsService = new MetricsService(
            monitoringConfig,
            "spanner",
            testConfig.getProperty("spanner.emulator.project")
        );

        BenchmarkProfile profile = new BenchmarkProfile(
            Integer.parseInt(testConfig.getProperty("test.batch.size")),
            Integer.parseInt(testConfig.getProperty("test.total.operations")),
            Duration.ofMinutes(Integer.parseInt(testConfig.getProperty("test.duration.minutes")))
        );

        BatchStatementExecutor executor = new BatchStatementExecutor(
            spannerConfig,
            profile,
            metricsService
        );

        assertDoesNotThrow(() -> executor.execute());
    }
} 