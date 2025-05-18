package com.microbenchmark.benchmark;

import com.microbenchmark.config.BenchmarkProfile;
import com.microbenchmark.config.DatabaseConfig;
import com.microbenchmark.config.MonitoringConfig;
import com.microbenchmark.config.SpannerConfig;
import com.microbenchmark.config.SpannerConnectionType;
import com.microbenchmark.metrics.MetricsService;
import com.microbenchmark.test.EmulatorTestHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BenchmarkIntegrationTest {
    private EmulatorTestHelper emulatorHelper;
    private Properties testConfig;
    private MonitoringConfig monitoringConfig;
    private MetricsService metricsService;

    @BeforeAll
    void setUp() {
        emulatorHelper = new EmulatorTestHelper();
        emulatorHelper.startSpannerEmulator();
        testConfig = emulatorHelper.getTestConfig();

        monitoringConfig = new MonitoringConfig(
            Boolean.parseBoolean(testConfig.getProperty("metrics.enabled", "false")),
            testConfig.getProperty("google.cloud.project.id", ""),
            testConfig.getProperty("metrics.prefix", "test")
        );

        metricsService = new MetricsService(
            monitoringConfig,
            "test",
            testConfig.getProperty("google.cloud.project.id", "")
        );
    }

    @AfterAll
    void tearDown() {
        if (emulatorHelper != null) {
            emulatorHelper.close();
        }
    }

    @Test
    void testSpannerDirectJdbc() {
        DatabaseConfig config = new SpannerConfig(
            testConfig.getProperty("spanner.emulator.project"),
            testConfig.getProperty("spanner.emulator.instance"),
            testConfig.getProperty("spanner.emulator.database"),
            emulatorHelper.getSpannerEmulatorHost(),
            emulatorHelper.getSpannerEmulatorPort(),
            SpannerConnectionType.JDBC_DIRECT
        );

        runBenchmark(config);
    }

    @Test
    void testSpannerPgAdapter() {
        DatabaseConfig config = new SpannerConfig(
            testConfig.getProperty("spanner.emulator.project"),
            testConfig.getProperty("spanner.emulator.instance"),
            testConfig.getProperty("spanner.emulator.database"),
            emulatorHelper.getSpannerEmulatorHost(),
            emulatorHelper.getPgAdapterPort(),
            SpannerConnectionType.PGADAPTER_JDBC
        );

        runBenchmark(config);
    }

    private void runBenchmark(DatabaseConfig config) {
        BenchmarkProfile profile = new BenchmarkProfile(
            Integer.parseInt(testConfig.getProperty("test.batch.size", "10")),
            Integer.parseInt(testConfig.getProperty("test.total.operations", "100")),
            Duration.ofMinutes(Integer.parseInt(testConfig.getProperty("test.duration.minutes", "1")))
        );

        BatchStatementExecutor executor = new BatchStatementExecutor(config, profile, metricsService);
        assertDoesNotThrow(() -> executor.execute());
    }
} 