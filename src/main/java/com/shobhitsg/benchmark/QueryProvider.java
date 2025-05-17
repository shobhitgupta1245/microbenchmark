package com.shobhitsg.benchmark;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface QueryProvider {
    String getSql();
    
    /**
     * Sets parameters for a single batch entry in the prepared statement
     * @param stmt The prepared statement
     * @param batchIndex The current batch index (0-based)
     * @throws SQLException if parameter setting fails
     */
    void setParameters(PreparedStatement stmt, long batchIndex) throws SQLException;
    
    /**
     * @return The size of each batch
     */
    int getBatchSize();
    
    /**
     * @return The total number of operations to perform
     */
    long getTotalOperations();
} 