package com.shobhitsg.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationLoader.class);

    public static DatabaseConfig loadSpannerConfig() {
        Properties props = loadProperties("spanner.properties");
        return new SpannerConfig(
                props.getProperty("project.id"),
                props.getProperty("instance.id"),
                props.getProperty("database.id")
        );
    }

    public static DatabaseConfig loadPostgresConfig() {
        Properties props = loadProperties("postgres.properties");
        return new PostgresConfig(
                props.getProperty("host"),
                Integer.parseInt(props.getProperty("port")),
                props.getProperty("database"),
                props.getProperty("username"),
                props.getProperty("password")
        );
    }

    private static Properties loadProperties(String resourceName) {
        Properties props = new Properties();
        try (InputStream input = ConfigurationLoader.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (input == null) {
                throw new RuntimeException("Unable to find " + resourceName);
            }
            props.load(input);
        } catch (IOException e) {
            logger.error("Error loading properties from " + resourceName, e);
            throw new RuntimeException("Failed to load configuration", e);
        }
        return props;
    }
} 