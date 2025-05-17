package com.microbenchmark.config;

public enum BenchmarkProfile {
    SPANNER("spanner"),
    POSTGRES("postgres");

    private final String value;

    BenchmarkProfile(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static BenchmarkProfile fromString(String profile) {
        for (BenchmarkProfile bp : values()) {
            if (bp.value.equalsIgnoreCase(profile)) {
                return bp;
            }
        }
        throw new IllegalArgumentException("Unknown profile: " + profile + ". Valid profiles are: spanner, postgres");
    }
} 