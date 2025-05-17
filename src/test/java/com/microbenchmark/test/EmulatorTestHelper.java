package com.microbenchmark.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class EmulatorTestHelper {
    private static final Logger logger = LoggerFactory.getLogger(EmulatorTestHelper.class);
    private static final String POSTGRES_SCHEMA_FILE = "schema.sql";
    private static final String SPANNER_SCHEMA_FILE = "spanner-schema.sql";
    private static final int SPANNER_PORT = 9010;
    private static final int PGADAPTER_PORT = 5432;
    private static final int REST_PORT = 9020;
    
    private PostgreSQLContainer<?> postgresContainer;
    private GenericContainer<?> spannerEmulator;
    private Properties testConfig;
    private final HttpClient httpClient;

    public EmulatorTestHelper() {
        loadTestConfig();
        this.httpClient = HttpClient.newHttpClient();
    }

    private void loadTestConfig() {
        testConfig = new Properties();
        try {
            testConfig.load(getClass().getClassLoader().getResourceAsStream("test-config.properties"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load test configuration", e);
        }
    }

    public void startPostgres() {
        postgresContainer = new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
            .withDatabaseName(testConfig.getProperty("postgres.test.database"))
            .withUsername(testConfig.getProperty("postgres.test.username"))
            .withPassword(testConfig.getProperty("postgres.test.password"));
        
        postgresContainer.start();
        initializePostgresSchema();
    }

    public void startSpannerEmulator() {
        spannerEmulator = new GenericContainer<>(DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator:latest"))
            .withExposedPorts(SPANNER_PORT, PGADAPTER_PORT, REST_PORT)
            .withEnv("SPANNER_PROJECT", testConfig.getProperty("spanner.emulator.project"))
            .withEnv("SPANNER_INSTANCE", testConfig.getProperty("spanner.emulator.instance"))
            // Enable pgAdapter with specific configuration
            .withEnv("ENABLE_PGADAPTER", "true")
            .withEnv("PGADAPTER_PORT", String.valueOf(PGADAPTER_PORT))
            .withEnv("PGADAPTER_ENABLE_AUTOCOMMIT", "true")
            .withEnv("PGADAPTER_ENABLE_IAM_AUTH", "false")
            .withEnv("PGADAPTER_ENABLE_SSL", "false")
            .withEnv("PGADAPTER_LISTEN_ADDRESSES", "*")
            // Configure REST and gRPC ports
            .withEnv("REST_PORT", String.valueOf(REST_PORT))
            .withEnv("GRPC_PORT", String.valueOf(SPANNER_PORT));
        
        spannerEmulator.start();

        // Wait a bit longer for the emulator to be fully ready
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        initializeSpannerSchema();
    }

    private void initializePostgresSchema() {
        try {
            String jdbcUrl = postgresContainer.getJdbcUrl();
            String username = postgresContainer.getUsername();
            String password = postgresContainer.getPassword();
            
            try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
                 Statement stmt = conn.createStatement()) {
                String schema = Files.readString(Path.of("src/main/resources/" + POSTGRES_SCHEMA_FILE));
                stmt.execute(schema);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize PostgreSQL schema", e);
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
            // Wait for the emulator to be ready
            Thread.sleep(2000);

            String restEndpoint = String.format("http://%s:%d",
                spannerEmulator.getHost(),
                spannerEmulator.getMappedPort(REST_PORT));

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

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Spanner emulator", e);
        }
    }

    public void stopAll() {
        if (postgresContainer != null) {
            postgresContainer.stop();
        }
        if (spannerEmulator != null) {
            spannerEmulator.stop();
        }
    }

    public String getPostgresJdbcUrl() {
        return postgresContainer != null ? postgresContainer.getJdbcUrl() : null;
    }

    public String getPostgresUsername() {
        return postgresContainer != null ? postgresContainer.getUsername() : null;
    }

    public String getPostgresPassword() {
        return postgresContainer != null ? postgresContainer.getPassword() : null;
    }

    public String getSpannerEmulatorHost() {
        return spannerEmulator != null ? spannerEmulator.getHost() : null;
    }

    public Integer getSpannerEmulatorPort() {
        return spannerEmulator != null ? spannerEmulator.getMappedPort(SPANNER_PORT) : null;
    }

    public Integer getPgAdapterPort() {
        return spannerEmulator != null ? spannerEmulator.getMappedPort(PGADAPTER_PORT) : null;
    }

    public Properties getTestConfig() {
        return testConfig;
    }
} 