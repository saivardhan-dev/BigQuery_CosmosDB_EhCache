package com.dataplatform.bq_cdb_ch.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * SpringDoc / OpenAPI 3 configuration.
 * Swagger UI: /swagger-ui.html
 * OpenAPI JSON: /api-docs
 */
@Configuration
public class OpenApiConfig {

    @Value("${server.port:8080}")
    private int serverPort;

    @Bean
    public OpenAPI bqCdbChOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("BigQuery → CosmosDB → EhCache Pipeline API")
                        .version("1.0.0")
                        .description("""
                                ## Data Platform Pipeline Microservice

                                ### Architecture
                                ```
                                Google BigQuery ──► Azure Cosmos DB ──► EhCache ──► REST APIs
                                   (read-only)       (source of truth)   (native)
                                ```

                                ### Cache-Aside Pattern
                                All GET endpoints check EhCache first. On cache miss, data is
                                fetched from Cosmos DB and the cache is populated for subsequent reads.

                                ### Sync Pipeline
                                - Scheduled: every 2 hours (configurable via `scheduler.sync.cron`)
                                - Manual trigger: `POST /api/v1/sync/trigger`
                                """)
                        .contact(new Contact()
                                .name("Data Platform Engineering")
                                .email("dataplatform@yourcompany.com"))
                        .license(new License().name("Internal Use Only")))
                .servers(List.of(
                        new Server().url("http://localhost:" + serverPort).description("Local Development"),
                        new Server().url("https://bq-cdb-ch.internal.yourcompany.com").description("Production")
                ));
    }
}