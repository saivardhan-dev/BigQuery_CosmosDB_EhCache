package com.dataplatform.bq_cdb_ch.config;

import com.azure.cosmos.CosmosAsyncClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Actuator health indicator for Azure Cosmos DB.
 * Exposes status at /actuator/health under the "cosmos" component.
 */
@Slf4j
@Component("cosmos")
@RequiredArgsConstructor
public class CosmosHealthIndicator implements HealthIndicator {

    private final CosmosAsyncClient cosmosAsyncClient;

    @Value("${azure.cosmos.database}")
    private String databaseName;

    @Value("${azure.cosmos.endpoint}")
    private String cosmosEndpoint;

    @Override
    public Health health() {
        try {
            // Lightweight probe: read database properties with 5s timeout
            cosmosAsyncClient
                    .getDatabase(databaseName)
                    .read()
                    .timeout(Duration.ofSeconds(5))
                    .block();

            return Health.up()
                    .withDetail("database", databaseName)
                    .withDetail("endpoint", extractHost(cosmosEndpoint))
                    .build();

        } catch (Exception e) {
            log.warn("Cosmos DB health check failed: {}", e.getMessage());
            return Health.down()
                    .withDetail("database", databaseName)
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }

    private String extractHost(String uri) {
        try {
            return new java.net.URI(uri).getHost();
        } catch (Exception e) {
            return "unknown";
        }
    }
}