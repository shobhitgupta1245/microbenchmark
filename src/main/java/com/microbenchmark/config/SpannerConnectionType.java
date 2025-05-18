package com.microbenchmark.config;

public enum SpannerConnectionType {
    JDBC_DIRECT,    // Direct Spanner JDBC connection
    PGADAPTER_JDBC  // PostgreSQL JDBC via pgAdapter
} 