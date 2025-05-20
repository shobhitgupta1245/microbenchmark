package com.microbenchmark.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationLoader {
    private static final String CONFIG_FILE = "spanner.properties";

    public static Properties loadConfig() {
        Properties props = new Properties();
        try (InputStream input = ConfigurationLoader.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                throw new RuntimeException("Unable to find " + CONFIG_FILE);
            }
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
        return props;
    }

    public static DatabaseConfig createSpannerConfig(Properties config) {
        return new SpannerConfig(
            config.getProperty("project.id"),
            config.getProperty("instance.id"),
            config.getProperty("database.id"),
            SpannerConnectionType.JDBC_DIRECT  // Default to direct JDBC
        );
    }

    public static DatabaseConfig createPostgresConfig(Properties config) {
        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s",
            config.getProperty("postgres.host", "localhost"),
            config.getProperty("postgres.port", "5432"),
            config.getProperty("postgres.database")
        );
        return new PostgresConfig(
            jdbcUrl,
            config.getProperty("postgres.username"),
            config.getProperty("postgres.password")
        );
    }
} 