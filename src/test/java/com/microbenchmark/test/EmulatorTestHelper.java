package com.microbenchmark.test;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class EmulatorTestHelper implements AutoCloseable {
    private static final String SPANNER_SCHEMA_FILE = "spanner-schema.sql";
    private static final int SPANNER_PORT = 9010;
    private static final int PGADAPTER_PORT = 5432;
    private static final int REST_PORT = 9020;
    private static final String DOCKER_IMAGE = "gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator";
    
    private GenericContainer<?> emulatorContainer;
    private Properties testConfig;
    private final HttpClient httpClient;

    public EmulatorTestHelper() {
        this.httpClient = HttpClient.newHttpClient();
    }

    public void startSpannerEmulator() {
        // Initialize test configuration first
        testConfig = new Properties();
        testConfig.setProperty("spanner.emulator.project", "test-project");
        testConfig.setProperty("spanner.emulator.instance", "test-instance");
        testConfig.setProperty("spanner.emulator.database", "test-database");
        testConfig.setProperty("test.batch.size", "10");
        testConfig.setProperty("test.total.operations", "100");
        testConfig.setProperty("test.duration.minutes", "1");
        testConfig.setProperty("metrics.enabled", "false");

        // Start container with all required ports
        emulatorContainer = new GenericContainer<>(DockerImageName.parse(DOCKER_IMAGE));
        emulatorContainer.addExposedPort(SPANNER_PORT);
        emulatorContainer.addExposedPort(PGADAPTER_PORT);
        emulatorContainer.addExposedPort(REST_PORT);
        emulatorContainer.setWaitStrategy(Wait.forListeningPorts(PGADAPTER_PORT));
        emulatorContainer.start();

        // Wait for the emulator to be fully ready
        try {
            Thread.sleep(10000); // Wait 10 seconds for the emulator to stabilize
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Verify REST endpoint is accessible before proceeding
        verifyRestEndpoint();

        // Initialize schema after container is ready
        initializeSpannerSchema();
    }

    private void verifyRestEndpoint() {
        String restEndpoint = String.format("http://%s:%d",
            emulatorContainer.getHost(),
            emulatorContainer.getMappedPort(REST_PORT));

        HttpRequest healthCheck = HttpRequest.newBuilder()
            .uri(URI.create(restEndpoint))
            .GET()
            .build();

        try {
            int maxRetries = 5;
            int retryCount = 0;
            boolean isHealthy = false;

            while (!isHealthy && retryCount < maxRetries) {
                try {
                    HttpResponse<String> response = httpClient.send(healthCheck, HttpResponse.BodyHandlers.ofString());
                    isHealthy = response.statusCode() == 200;
                } catch (Exception e) {
                    Thread.sleep(2000); // Wait 2 seconds before retry
                }
                retryCount++;
            }

            if (!isHealthy) {
                throw new RuntimeException("REST endpoint failed to become available");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to verify REST endpoint", e);
        }
    }

    private List<String> parseSchemaStatements(String schema) {
        return Arrays.stream(schema.split(";"))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> s.replaceAll("--[^\\n]*\\n", "")) // Remove comments
            .map(s -> s.replaceAll("\\s+", " ")) // Normalize whitespace
            .collect(Collectors.toList());
    }

    private void initializeSpannerSchema() {
        try {
            String restEndpoint = String.format("http://%s:%d",
                emulatorContainer.getHost(),
                emulatorContainer.getMappedPort(REST_PORT));

            // Create instance
            String createInstanceUrl = String.format("%s/v1/projects/%s/instances",
                restEndpoint,
                testConfig.getProperty("spanner.emulator.project"));
            
            String instanceBody = String.format("""
                {
                    "instance": {
                        "name": "projects/%s/instances/%s",
                        "config": "emulator-config",
                        "displayName": "Test Instance",
                        "nodeCount": 1,
                        "state": "READY"
                    },
                    "instanceId": "%s"
                }""",
                testConfig.getProperty("spanner.emulator.project"),
                testConfig.getProperty("spanner.emulator.instance"),
                testConfig.getProperty("spanner.emulator.instance"));

        
            HttpRequest createInstanceRequest = HttpRequest.newBuilder()
                .uri(URI.create(createInstanceUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(instanceBody))
                .build();

            HttpResponse<String> instanceResponse = httpClient.send(createInstanceRequest,
                HttpResponse.BodyHandlers.ofString());

            if (instanceResponse.statusCode() != 200 && instanceResponse.statusCode() != 409) {
                throw new RuntimeException("Failed to create instance: " + instanceResponse.body());
            }
            

            // Wait after instance creation
            Thread.sleep(2000);

            // Create database
            String createDatabaseUrl = String.format("%s/v1/projects/%s/instances/%s/databases",
                restEndpoint,
                testConfig.getProperty("spanner.emulator.project"),
                testConfig.getProperty("spanner.emulator.instance"));

            String schema = Files.readString(Path.of("src/main/resources/" + SPANNER_SCHEMA_FILE));
            List<String> statements = parseSchemaStatements(schema);

            String databaseBody = String.format("""
                {
                    "createStatement": "CREATE DATABASE `%s`",
                    "extraStatements": %s
                }""",
                testConfig.getProperty("spanner.emulator.database"),
                statements.stream()
                    .map(s -> "\"" + s.replace("\"", "\\\"") + "\"")
                    .collect(Collectors.joining(",", "[", "]")));

            HttpRequest createDatabaseRequest = HttpRequest.newBuilder()
                .uri(URI.create(createDatabaseUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(databaseBody))
                .build();

            HttpResponse<String> databaseResponse = httpClient.send(createDatabaseRequest,
                HttpResponse.BodyHandlers.ofString());

            if (databaseResponse.statusCode() != 200 && databaseResponse.statusCode() != 409) {
                throw new RuntimeException("Failed to create database: " + databaseResponse.body());
            }

            // Wait after database creation
            Thread.sleep(2000);

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Spanner emulator", e);
        }
    }

    @Override
    public void close() {
        if (emulatorContainer != null) {
            emulatorContainer.stop();
        }
        if (httpClient instanceof AutoCloseable) {
            try {
                ((AutoCloseable) httpClient).close();
            } catch (Exception e) {
                // Ignore
            }
        }
    }

    public String getSpannerEmulatorHost() {
        return emulatorContainer.getHost();
    }

    public int getSpannerEmulatorPort() {
        return emulatorContainer.getMappedPort(SPANNER_PORT);
    }

    public int getPgAdapterPort() {
        return emulatorContainer.getMappedPort(PGADAPTER_PORT);
    }

    public Properties getTestConfig() {
        return testConfig;
    }
} 