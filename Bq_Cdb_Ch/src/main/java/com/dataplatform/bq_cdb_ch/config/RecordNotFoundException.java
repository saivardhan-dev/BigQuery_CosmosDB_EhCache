package com.dataplatform.bq_cdb_ch.config;

/**
 * Thrown when a requested Record cannot be found in Cosmos DB or cache.
 * Using a custom exception avoids a dependency on jakarta.persistence-api
 * (which is not on the classpath without spring-boot-starter-data-jpa).
 */
public class RecordNotFoundException extends RuntimeException {

    public RecordNotFoundException(String id) {
        super("Record not found with id: " + id);
    }

    public RecordNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}