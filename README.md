# Database Performance Benchmarking Tool

A comprehensive benchmarking system to compare Cloud Spanner JDBC with PostgreSQL + pgAdapter performance.

## Features

- Multi-table schema (users and orders)
- Variable batch size processing (5-15 statements)
- Multiple operation types support:
  - User inserts
  - Order inserts
  - User updates
  - Order updates
  - Mixed operations
- Detailed performance metrics:
  - Latency percentiles (P50-P99)
  - Per-batch-size statistics
  - Throughput measurements
  - Success/failure tracking

## Prerequisites

- Java 17 or higher
- Maven
- PostgreSQL
- Cloud Spanner instance
- pgAdapter (for PostgreSQL testing)

## Configuration

1. Update `src/main/resources/spanner.properties` with your Cloud Spanner credentials:
   - project.id
   - instance.id
   - database.id

2. Update `src/main/resources/postgres.properties` with your PostgreSQL configuration:
   - host
   - port
   - database
   - username
   - password

## Building

```bash
mvn clean install
```

## Running

```bash
mvn exec:java -Dexec.mainClass="com.shobhitsg.Main"
```

## Schema

The benchmark uses two tables:
- `users`: Stores user information
- `orders`: Stores order information with foreign key to users

See `src/main/resources/schema.sql` for complete schema definition.

## Performance Tuning

Both database configurations include performance tuning parameters in their respective properties files:

- Spanner: Connection pools, channels, batching configurations
- PostgreSQL: Connection settings, buffer sizes, pgAdapter-specific optimizations 