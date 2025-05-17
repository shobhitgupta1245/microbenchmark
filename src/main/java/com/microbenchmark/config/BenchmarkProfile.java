package com.microbenchmark.config;

import java.time.Duration;

public class BenchmarkProfile {
    private final int batchSize;
    private final int totalOperations;
    private final Duration maxDuration;

    public BenchmarkProfile(int batchSize, int totalOperations, Duration maxDuration) {
        this.batchSize = batchSize;
        this.totalOperations = totalOperations;
        this.maxDuration = maxDuration;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getTotalOperations() {
        return totalOperations;
    }

    public Duration getMaxDuration() {
        return maxDuration;
    }
} 