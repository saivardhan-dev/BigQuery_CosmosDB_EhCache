package com.dataplatform.bq_cdb_ch.service;

import com.dataplatform.bq_cdb_ch.dto.SyncResult;
import com.dataplatform.bq_cdb_ch.dto.SyncResult.StepResult;
import com.dataplatform.bq_cdb_ch.dto.SyncResult.SyncStatus;
import com.dataplatform.bq_cdb_ch.model.Record;
import com.dataplatform.bq_cdb_ch.repository.RecordRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Orchestrates the full data sync pipeline:
 *
 *   Google BigQuery → Azure Cosmos DB → EhCache
 *
 * Key performance settings (application.yml):
 *
 *   batch-pipeline.chunk-size      Records per saveAll() call.
 *                                   400 RU/s  → 100
 *                                   1000 RU/s → 200
 *                                   10000 RU/s → 500-1000
 *
 *   batch-pipeline.throttle-limit  Parallel write threads.
 *                                   400 RU/s  → 4
 *                                   1000 RU/s → 8
 *                                   10000 RU/s → 16
 *
 * Why parallel chunks matter:
 *   Sequential writes at 400 RU/s with chunk=1000 → ~30s per chunk → ~8 hours for 1M records.
 *   Parallel writes at 400 RU/s with chunk=100, threads=4 → ~34 minutes for 1M records.
 *   The parallel threads saturate the RU budget without exceeding it per-call.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DataSyncService {

    private final BigQueryService  bigQueryService;
    private final RecordRepository recordRepository;
    private final EhCacheService   cacheService;
    private final MeterRegistry    meterRegistry;

    @Value("${batch-pipeline.chunk-size:100}")
    private int chunkSize;

    @Value("${batch-pipeline.throttle-limit:4}")
    private int throttleLimit;

    @Value("${batch-pipeline.max-retries:3}")
    private int maxRetries;

    @Value("${batch-pipeline.retry-backoff-ms:2000}")
    private long retryBackoffMs;

    // ── Public API ─────────────────────────────────────────────────────────

    public SyncResult executeFullSync() {
        Instant startedAt = Instant.now();
        log.info("=== Full Data Sync Started (chunk={}, threads={}) ===",
                chunkSize, throttleLimit);

        Map<String, StepResult> steps    = new LinkedHashMap<>();
        List<String>            warnings = Collections.synchronizedList(new ArrayList<>());

        // ── Step 1: Fetch from BigQuery ───────────────────────────────────
        List<Record> records;
        long stepStart = System.currentTimeMillis();
        try {
            records = bigQueryService.fetchAllRecords();
        } catch (Exception e) {
            log.error("Step 1 FAILED unexpectedly: {}", e.getMessage(), e);
            steps.put("bigquery-fetch", StepResult.builder()
                    .stepName("bigquery-fetch").status(SyncStatus.FAILED)
                    .recordsProcessed(0).durationMs(System.currentTimeMillis() - stepStart)
                    .errorMessage(e.getMessage()).build());
            return buildResult(SyncStatus.FAILED, 0, 0, 0, 0,
                    startedAt, steps, warnings, e.getMessage());
        }

        if (records.isEmpty()) {
            String msg = "BigQuery returned 0 records — circuit breaker may be OPEN " +
                    "or all retry attempts failed. Aborting sync.";
            log.error("Step 1 FAILED: {}", msg);
            steps.put("bigquery-fetch", StepResult.builder()
                    .stepName("bigquery-fetch").status(SyncStatus.FAILED)
                    .recordsProcessed(0).durationMs(System.currentTimeMillis() - stepStart)
                    .errorMessage(msg).build());
            return buildResult(SyncStatus.FAILED, 0, 0, 0, 0,
                    startedAt, steps, warnings, msg);
        }

        steps.put("bigquery-fetch", StepResult.builder()
                .stepName("bigquery-fetch").status(SyncStatus.SUCCESS)
                .recordsProcessed(records.size())
                .durationMs(System.currentTimeMillis() - stepStart).build());
        log.info("Step 1 complete: {} records from BigQuery.", records.size());

        long totalRead = records.size();

        // ── Step 2: Parallel batch upsert into Cosmos DB ─────────────────
        AtomicLong writtenToCosmos = new AtomicLong(0);
        AtomicLong failedRecords   = new AtomicLong(0);
        StepResult cosmosStep = runCosmosUpsertParallel(
                records, writtenToCosmos, failedRecords, warnings);
        steps.put("cosmos-upsert", cosmosStep);

        if (cosmosStep.getStatus() == SyncStatus.FAILED) {
            log.error("Step 2 FAILED — skipping cache load.");
            return buildResult(SyncStatus.FAILED, totalRead,
                    writtenToCosmos.get(), 0, failedRecords.get(),
                    startedAt, steps, warnings, cosmosStep.getErrorMessage());
        }

        // ── Step 3: Bulk load into EhCache ────────────────────────────────
        AtomicLong loadedIntoCache = new AtomicLong(0);
        StepResult cacheStep;
        long cacheStart = System.currentTimeMillis();
        try {
            cacheService.clearAll();
            cacheService.bulkLoad(records);
            loadedIntoCache.set(records.size());
            cacheStep = StepResult.builder()
                    .stepName("cache-load").status(SyncStatus.SUCCESS)
                    .recordsProcessed(records.size())
                    .durationMs(System.currentTimeMillis() - cacheStart).build();
            log.info("Step 3 complete: {} records loaded into EhCache.", records.size());
        } catch (Exception e) {
            log.error("Step 3 FAILED (cache load) — non-fatal: {}", e.getMessage(), e);
            warnings.add("Cache load failed — reads fall back to Cosmos DB until next sync.");
            cacheStep = StepResult.builder()
                    .stepName("cache-load").status(SyncStatus.FAILED)
                    .recordsProcessed(0)
                    .durationMs(System.currentTimeMillis() - cacheStart)
                    .errorMessage(e.getMessage()).build();
        }
        steps.put("cache-load", cacheStep);

        // ── Overall status ────────────────────────────────────────────────
        SyncStatus overallStatus;
        if (failedRecords.get() == 0 && cacheStep.getStatus() == SyncStatus.SUCCESS) {
            overallStatus = SyncStatus.SUCCESS;
        } else if (writtenToCosmos.get() > 0) {
            overallStatus = SyncStatus.PARTIAL_SUCCESS;
        } else {
            overallStatus = SyncStatus.FAILED;
        }

        meterRegistry.counter("sync.executions", "status", overallStatus.name()).increment();
        meterRegistry.gauge("sync.last.records_read",    totalRead);
        meterRegistry.gauge("sync.last.records_written", writtenToCosmos.get());

        SyncResult result = buildResult(overallStatus, totalRead,
                writtenToCosmos.get(), loadedIntoCache.get(), failedRecords.get(),
                startedAt, steps, warnings, null);

        log.info("=== Sync Complete: {} | read={} written={} cached={} failed={} | {}ms ===",
                overallStatus, totalRead, writtenToCosmos.get(),
                loadedIntoCache.get(), failedRecords.get(), result.getDurationMs());

        return result;
    }

    // ── Private helpers ────────────────────────────────────────────────────

    /**
     * Writes chunks to Cosmos DB in parallel using a fixed thread pool.
     *
     * throttle-limit controls how many chunks are written concurrently.
     * This saturates the provisioned RU/s without exceeding it per-call,
     * which is the key to fast bulk loads.
     *
     *   400 RU/s  + chunk=100 + threads=4  → ~34 min for 1M records
     *   1000 RU/s + chunk=200 + threads=8  → ~8 min for 1M records
     *   10000 RU/s + chunk=500 + threads=16 → ~2 min for 1M records
     */
    private StepResult runCosmosUpsertParallel(List<Record> records, AtomicLong written,
                                               AtomicLong failed, List<String> warnings) {
        long start = System.currentTimeMillis();
        List<List<Record>> chunks = partition(records, chunkSize);
        int totalChunks = chunks.size();

        log.info("Step 2: Cosmos upsert — {} records, chunk={}, threads={}, total_chunks={}",
                records.size(), chunkSize, throttleLimit, totalChunks);

        ExecutorService executor = Executors.newFixedThreadPool(throttleLimit);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < chunks.size(); i++) {
            final List<Record> chunk = chunks.get(i);
            final int chunkIdx = i + 1;

            futures.add(executor.submit(() -> {
                boolean success = false;
                Exception lastEx = null;

                for (int attempt = 1; attempt <= maxRetries; attempt++) {
                    try {
                        recordRepository.saveAll(chunk);
                        written.addAndGet(chunk.size());
                        success = true;
                        if (chunkIdx % 50 == 0 || chunkIdx <= 5) {
                            log.info("Chunk {}/{} written ({} records, total written: {})",
                                    chunkIdx, totalChunks, chunk.size(), written.get());
                        } else {
                            log.debug("Chunk {}/{} written ({} records)",
                                    chunkIdx, totalChunks, chunk.size());
                        }
                        break;
                    } catch (Exception e) {
                        lastEx = e;
                        log.warn("Chunk {}/{} attempt {}/{} failed: {}",
                                chunkIdx, totalChunks, attempt, maxRetries, e.getMessage());
                        if (attempt < maxRetries) sleep(retryBackoffMs * attempt);
                    }
                }

                if (!success) {
                    failed.addAndGet(chunk.size());
                    String w = String.format("Chunk %d/%d failed after %d retries: %s",
                            chunkIdx, totalChunks, maxRetries,
                            lastEx != null ? lastEx.getMessage() : "unknown");
                    warnings.add(w);
                    log.error(w);
                }
            }));
        }

        // Wait for all chunks to complete
        executor.shutdown();
        try {
            executor.awaitTermination(2, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Cosmos upsert interrupted");
        }

        SyncStatus status = failed.get() == 0 ? SyncStatus.SUCCESS : SyncStatus.PARTIAL_SUCCESS;
        long durationMs = System.currentTimeMillis() - start;
        log.info("Step 2 complete: written={} failed={} duration={}ms",
                written.get(), failed.get(), durationMs);

        return StepResult.builder()
                .stepName("cosmos-upsert").status(status)
                .recordsProcessed(written.get())
                .durationMs(durationMs).build();
    }

    private SyncResult buildResult(SyncStatus status, long read, long written,
                                   long cached, long failed, Instant startedAt,
                                   Map<String, StepResult> steps,
                                   List<String> warnings, String errorMessage) {
        Instant now = Instant.now();
        return SyncResult.builder()
                .status(status).jobId(UUID.randomUUID().toString())
                .totalReadFromBigQuery(read).totalWrittenToCosmos(written)
                .totalLoadedIntoCache(cached).failedRecords(failed)
                .startedAt(startedAt).completedAt(now)
                .durationMs(now.toEpochMilli() - startedAt.toEpochMilli())
                .steps(steps)
                .warnings(warnings.isEmpty() ? null : warnings)
                .errorMessage(errorMessage)
                .build();
    }

    private <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> result = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            result.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return result;
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}