package com.microbenchmark.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationLoader.class);
    private static final String CONFIG_FILE = "config.properties";

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
            config.getProperty("spanner.project.id"),
            config.getProperty("spanner.instance.id"),
            config.getProperty("spanner.database.id"),
            config.getProperty("spanner.host", "localhost"),
            Integer.parseInt(config.getProperty("spanner.port", "9010"))
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