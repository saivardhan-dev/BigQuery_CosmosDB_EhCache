package com.dataplatform.bq_cdb_ch.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Configures the Google BigQuery client.
 *
 * Authentication priority:
 *  1. If {@code bigquery.credentials-path} is set → load service account JSON from that path.
 *  2. Otherwise → use Application Default Credentials (ADC).
 *     ADC works automatically on GKE (Workload Identity) and locally via
 *     {@code gcloud auth application-default login}.
 */
@Slf4j
@Configuration
public class BigQueryConfig {

    @Value("${bigquery.project-id}")
    private String projectId;

    @Value("${bigquery.credentials-path:}")
    private String credentialsPath;

    @Bean
    public BigQuery bigQuery() throws IOException {
        BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder()
                .setProjectId(projectId);

        if (StringUtils.hasText(credentialsPath)) {
            log.info("BigQuery: loading credentials from file: {}", credentialsPath);
            try (FileInputStream fis = new FileInputStream(credentialsPath)) {
                GoogleCredentials credentials = ServiceAccountCredentials.fromStream(fis);
                optionsBuilder.setCredentials(credentials);
            }
        } else {
            log.info("BigQuery: using Application Default Credentials (ADC)");
            optionsBuilder.setCredentials(GoogleCredentials.getApplicationDefault());
        }

        BigQuery bigQuery = optionsBuilder.build().getService();
        log.info("BigQuery client initialized for project: {}", projectId);
        return bigQuery;
    }
}