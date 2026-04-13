package com.dataplatform.bq_cdb_ch.service;

import com.dataplatform.bq_cdb_ch.model.Record;
import com.google.cloud.bigquery.*;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Fetches records from Google BigQuery and maps them to {@link Record}.
 *
 * Resilience4j integration:
 *
 *   @Retry("bigquery")
 *     Retries up to 3 times with exponential backoff (2s → 4s → 8s) before
 *     propagating the exception. Handles transient BigQuery errors (rate limits,
 *     temporary network blips) without failing the sync pipeline.
 *
 *   @CircuitBreaker(name = "bigquery", fallbackMethod = "...")
 *     Wraps the retry-protected call. If 50% of the last 10 calls fail
 *     (after exhausting retries), the breaker OPENS and fast-fails all
 *     subsequent calls for 30 seconds — preventing thread exhaustion and
 *     cascading failures. A fallback method returns an empty list so the
 *     pipeline can report FAILED status cleanly instead of throwing.
 *
 *   Order of execution:
 *     Circuit Breaker → Retry → actual BigQuery call
 *   i.e. retry fires first (inside the breaker window), then if all
 *   retry attempts fail the breaker records one failure.
 *
 * BigQuery table schema (from CSV):
 *   id         STRING    "rec-0000001"
 *   name       STRING    "Product 1"
 *   category   STRING    "software" | "hardware" | … (10 values)
 *   value      INTEGER   19680
 *   updated_at TIMESTAMP "2026-04-01 09:00:00"
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BigQueryService {

    private final BigQuery bigQuery;

    @Value("${bigquery.project-id}")
    private String projectId;

    @Value("${bigquery.dataset}")
    private String dataset;

    @Value("${bigquery.table}")
    private String table;

    @Value("${bigquery.page-size:1000}")
    private int pageSize;

    @Value("${bigquery.query-timeout-seconds:120}")
    private long queryTimeoutSeconds;

    // ── Public API ─────────────────────────────────────────────────────────

    /**
     * Fetch all records from BigQuery.
     *
     * Resilience chain: CircuitBreaker → Retry → executeFullLoad()
     * Fallback: fetchAllRecordsFallback() — returns empty list, logs warning.
     */
    @CircuitBreaker(name = "bigquery", fallbackMethod = "fetchAllRecordsFallback")
    @Retry(name = "bigquery")
    public List<Record> fetchAllRecords() {
        return executeFullLoad();
    }

    /**
     * Fetch records updated since a given timestamp (incremental sync).
     *
     * Resilience chain: CircuitBreaker → Retry → executeIncrementalLoad()
     * Fallback: fetchRecordsSinceFallback() — returns empty list, logs warning.
     */
    @CircuitBreaker(name = "bigquery", fallbackMethod = "fetchRecordsSinceFallback")
    @Retry(name = "bigquery")
    public List<Record> fetchRecordsSince(Instant since) {
        return executeIncrementalLoad(since);
    }

    // ── Fallback methods ───────────────────────────────────────────────────
    //
    // Fallback method signatures MUST match the protected method exactly,
    // plus one additional Throwable parameter as the last argument.
    // Spring AOP routes here when the circuit is OPEN or all retries are exhausted.

    /**
     * Fallback for fetchAllRecords() — invoked when the circuit is OPEN
     * or all retry attempts have been exhausted.
     */
    @SuppressWarnings("unused")
    private List<Record> fetchAllRecordsFallback(Throwable t) {
        if (t instanceof CallNotPermittedException) {
            log.warn("BigQuery circuit breaker is OPEN — fast-failing fetchAllRecords(). " +
                    "Will retry after the wait-duration-in-open-state elapses.");
        } else {
            log.error("BigQuery fetchAllRecords() failed after all retries: {}", t.getMessage(), t);
        }
        // Return empty list — DataSyncService will detect 0 records and report FAILED status
        return List.of();
    }

    /**
     * Fallback for fetchRecordsSince(Instant) — same pattern.
     */
    @SuppressWarnings("unused")
    private List<Record> fetchRecordsSinceFallback(Instant since, Throwable t) {
        if (t instanceof CallNotPermittedException) {
            log.warn("BigQuery circuit breaker is OPEN — fast-failing fetchRecordsSince({}). ", since);
        } else {
            log.error("BigQuery fetchRecordsSince({}) failed after all retries: {}", since, t.getMessage(), t);
        }
        return List.of();
    }

    // ── Core execution methods (not annotated — called via public proxied methods) ──

    private List<Record> executeFullLoad() {
        String sql = buildIngestionQuery();
        log.info("BigQuery full-load: {}.{}.{}", projectId, dataset, table);
        log.debug("SQL: {}", sql);

        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql)
                    .setUseLegacySql(false)
                    .setAllowLargeResults(true)
                    .build();

            JobId jobId = JobId.of(UUID.randomUUID().toString());
            Job job = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

            job = job.waitFor(com.google.cloud.RetryOption.totalTimeout(
                    org.threeten.bp.Duration.ofSeconds(queryTimeoutSeconds)));

            if (job == null) {
                throw new RuntimeException("BigQuery job disappeared: " + jobId);
            }
            if (job.getStatus().getError() != null) {
                throw new RuntimeException("BigQuery job failed: " + job.getStatus().getError());
            }

            TableResult result = job.getQueryResults(
                    BigQuery.QueryResultsOption.pageSize(pageSize));

            List<Record> records = new ArrayList<>();
            long rowNum = 0;
            Instant now = Instant.now();

            for (FieldValueList row : result.iterateAll()) {
                try {
                    records.add(mapRow(row, now));
                    rowNum++;
                } catch (Exception e) {
                    log.warn("Skipping malformed row {}: {}", rowNum, e.getMessage());
                }
            }

            log.info("BigQuery full-load complete: {} records.", rowNum);
            return records;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("BigQuery query interrupted", e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("BigQuery fetch failed: " + e.getMessage(), e);
        }
    }

    private List<Record> executeIncrementalLoad(Instant since) {
        String sql = buildIncrementalQuery(since);
        log.info("BigQuery incremental load since {}", since);

        try {
            QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
                    .setUseLegacySql(false)
                    .build();
            TableResult result = bigQuery.query(config);

            List<Record> records = new ArrayList<>();
            Instant now = Instant.now();
            for (FieldValueList row : result.iterateAll()) {
                try { records.add(mapRow(row, now)); }
                catch (Exception e) { log.warn("Skipping row: {}", e.getMessage()); }
            }

            log.info("BigQuery incremental load complete: {} records since {}", records.size(), since);
            return records;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("BigQuery incremental query interrupted", e);
        }
    }

    // ── SQL builders ───────────────────────────────────────────────────────

    private String buildIngestionQuery() {
        return String.format("""
                SELECT
                    CAST(id AS STRING)         AS id,
                    name,
                    category,
                    CAST(value AS FLOAT64)     AS value,
                    TIMESTAMP(updated_at)      AS updated_at
                FROM `%s.%s.%s`
                ORDER BY id ASC
                """, projectId, dataset, table);
    }

    private String buildIncrementalQuery(Instant since) {
        return String.format("""
                SELECT
                    CAST(id AS STRING)         AS id,
                    name,
                    category,
                    CAST(value AS FLOAT64)     AS value,
                    TIMESTAMP(updated_at)      AS updated_at
                FROM `%s.%s.%s`
                WHERE updated_at > TIMESTAMP('%s')
                ORDER BY updated_at ASC
                """, projectId, dataset, table, since.toString());
    }

    // ── Row mapper ─────────────────────────────────────────────────────────

    private Record mapRow(FieldValueList row, Instant syncedAt) {
        return Record.builder()
                .id(getString(row, "id"))
                .name(getString(row, "name"))
                .category(getString(row, "category"))
                .value(getDouble(row, "value"))
                .updatedAt(getTimestamp(row, "updated_at", syncedAt))
                .syncedAt(syncedAt)
                .build();
    }

    // ── Field helpers ──────────────────────────────────────────────────────

    private String getString(FieldValueList row, String field) {
        try {
            FieldValue fv = row.get(field);
            return (fv == null || fv.isNull()) ? null : fv.getStringValue();
        } catch (Exception e) { return null; }
    }

    private Double getDouble(FieldValueList row, String field) {
        try {
            FieldValue fv = row.get(field);
            return (fv == null || fv.isNull()) ? null : fv.getDoubleValue();
        } catch (Exception e) { return null; }
    }

    private Instant getTimestamp(FieldValueList row, String field, Instant fallback) {
        try {
            FieldValue fv = row.get(field);
            if (fv == null || fv.isNull()) return fallback;
            long microseconds = fv.getTimestampValue();
            return Instant.ofEpochSecond(
                    microseconds / 1_000_000L,
                    (microseconds % 1_000_000L) * 1_000L);
        } catch (Exception e) { return fallback; }
    }
}