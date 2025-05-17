package com.microbenchmark.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MonitoringConfig {
    private static final Logger logger = LoggerFactory.getLogger(MonitoringConfig.class);
    private static final String CONFIG_FILE = "monitoring.properties";
    
    private final boolean enableCloudMonitoring;
    private final String projectId;
    private final String metricPrefix;

    public MonitoringConfig(boolean enableCloudMonitoring, String projectId, String metricPrefix) {
        this.enableCloudMonitoring = enableCloudMonitoring;
        this.projectId = projectId;
        this.metricPrefix = metricPrefix;
    }

    public boolean isCloudMonitoringEnabled() {
        return enableCloudMonitoring;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getMetricPrefix() {
        return metricPrefix;
    }

    public static MonitoringConfig load() {
        Properties props = new Properties();
        try (InputStream input = MonitoringConfig.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                logger.warn("Could not find {}. Using default configuration.", CONFIG_FILE);
                return getDefaultConfig();
            }
            props.load(input);
        } catch (IOException e) {
            logger.error("Error loading monitoring configuration", e);
            return getDefaultConfig();
        }

        String projectId = props.getProperty("google.cloud.project.id");
        boolean enabled = Boolean.parseBoolean(
            props.getProperty("metrics.enabled", "true")
        );
        String metricPrefix = props.getProperty("metrics.prefix", "");

        return new MonitoringConfig(enabled, projectId, metricPrefix);
    }

    private static MonitoringConfig getDefaultConfig() {
        return new MonitoringConfig(false, null, "");
    }
} 