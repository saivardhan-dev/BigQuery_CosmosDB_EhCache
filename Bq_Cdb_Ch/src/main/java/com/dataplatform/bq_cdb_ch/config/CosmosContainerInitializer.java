package com.dataplatform.bq_cdb_ch.config;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ExcludedPath;
import com.azure.cosmos.models.IncludedPath;
import com.azure.cosmos.models.IndexingMode;
import com.azure.cosmos.models.IndexingPolicy;
import com.azure.cosmos.models.ThroughputProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Ensures the Cosmos DB database and container exist at startup.
 * Runs once via ApplicationRunner — fully idempotent.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CosmosContainerInitializer implements ApplicationRunner {

    private final CosmosAsyncClient cosmosAsyncClient;

    @Value("${azure.cosmos.database}")
    private String databaseName;

    @Value("${azure.cosmos.container}")
    private String containerName;

    @Value("${azure.cosmos.partition-key:/id}")
    private String partitionKeyPath;

    @Override
    public void run(ApplicationArguments args) {
        log.info("Initializing Cosmos DB: database='{}' container='{}'", databaseName, containerName);
        try {
            ensureDatabaseExists();
            ensureContainerExists();
            log.info("Cosmos DB initialization complete.");
        } catch (Exception e) {
            // Non-fatal at startup — surface via /actuator/health
            log.error("Cosmos DB initialization failed: {}", e.getMessage(), e);
        }
    }

    private void ensureDatabaseExists() {
        cosmosAsyncClient
                .createDatabaseIfNotExists(databaseName)
                .block();
        log.debug("Database '{}' is ready.", databaseName);
    }

    private void ensureContainerExists() {
        CosmosContainerProperties containerProps =
                new CosmosContainerProperties(containerName, partitionKeyPath);

        // Cosmos DB indexing policy rules:
        //
        // When you define includedPaths, you MUST include "/*" (the mandatory root wildcard)
        // in at least one of the path sets, otherwise Cosmos rejects the policy with:
        // "The special mandatory indexing path "/" is not provided in any of the path type sets."
        //
        // Best practice for our schema:
        //   - Include "/*" to satisfy the mandatory requirement (indexes everything by default)
        //   - Exclude large/unused fields to save RU/s on writes
        //   - This is actually more efficient than listing individual includes,
        //     because Cosmos still only charges index RUs for non-excluded paths.
        IndexingPolicy indexingPolicy = new IndexingPolicy();
        indexingPolicy.setIndexingMode(IndexingMode.CONSISTENT);
        indexingPolicy.setAutomatic(true);

        // "/*" is the mandatory root wildcard — required by Cosmos DB
        indexingPolicy.setIncludedPaths(List.of(
                new IncludedPath("/*")
        ));

        // Exclude fields we never query on to reduce write RU consumption
        indexingPolicy.setExcludedPaths(List.of(
                new ExcludedPath("/syncedAt/?"),   // pipeline metadata — never queried
                new ExcludedPath("/_etag/?")       // managed internally by Cosmos
        ));

        containerProps.setIndexingPolicy(indexingPolicy);
        containerProps.setDefaultTimeToLiveInSeconds(-1); // no TTL

        cosmosAsyncClient
                .getDatabase(databaseName)
                .createContainerIfNotExists(containerProps,
                        ThroughputProperties.createManualThroughput(400))
                .block();

        log.debug("Container '{}' ready with partition key '{}'.", containerName, partitionKeyPath);
    }
}