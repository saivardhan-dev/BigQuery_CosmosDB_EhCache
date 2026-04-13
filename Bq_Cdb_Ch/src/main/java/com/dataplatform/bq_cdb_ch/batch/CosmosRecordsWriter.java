package com.dataplatform.bq_cdb_ch.batch;

import com.dataplatform.bq_cdb_ch.model.Record;
import com.dataplatform.bq_cdb_ch.repository.RecordRepository;
import com.dataplatform.bq_cdb_ch.service.EhCacheService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Spring Batch ItemWriter that upserts Records into Azure Cosmos DB.
 *
 * FIX: "incompatible types: inference variable S has incompatible equality
 *       constraints Record, capture#1 of ? extends Record"
 *
 *   Root cause: Spring Batch 5 defines ItemWriter<T> and Chunk<T>.
 *   When the class is declared as {@code implements ItemWriter<Record>},
 *   the write() method signature must be:
 *
 *       public void write(Chunk<? extends Record> chunk)
 *
 *   BUT calling recordRepository.saveAll(chunk.getItems()) fails because
 *   saveAll() expects Iterable<S extends Record> and the compiler cannot
 *   unify "? extends Record" with the concrete Record type expected by
 *   CosmosRepository.saveAll().
 *
 *   Fix: copy chunk items into a plain {@code List<Record>} before passing
 *   to saveAll(). This resolves the wildcard capture mismatch without any
 *   unchecked casts.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CosmosRecordsWriter implements ItemWriter<Record> {

    private final RecordRepository recordRepository;
    private final MeterRegistry    meterRegistry;

    @Override
    public void write(Chunk<? extends Record> chunk) throws Exception {
        if (chunk.isEmpty()) return;

        // FIX: Copy wildcard-typed chunk items into a concrete List<Record>.
        // This resolves the type inference conflict between Chunk<? extends Record>
        // and CosmosRepository.saveAll(Iterable<S>).
        List<Record> records = new ArrayList<>(chunk.getItems());

        log.debug("CosmosRecordsWriter: writing {} records", records.size());
        long start = System.currentTimeMillis();

        try {
            recordRepository.saveAll(records);
            meterRegistry.counter("cosmos.writes.success").increment(records.size());
            log.debug("CosmosRecordsWriter: wrote {} records in {}ms",
                    records.size(), System.currentTimeMillis() - start);
        } catch (Exception e) {
            meterRegistry.counter("cosmos.writes.failure").increment(records.size());
            log.error("CosmosRecordsWriter: chunk write failed ({} records): {}",
                    records.size(), e.getMessage(), e);
            throw e; // propagate to Spring Batch retry/skip
        }
    }

    /**
     * Called by cacheWarmUpStep after all Cosmos writes complete.
     * Loads all records from Cosmos DB into EhCache.
     */
    public void warmUpCache(EhCacheService cacheService) {
        log.info("Cache warm-up: loading all records from Cosmos DB...");
        long start = System.currentTimeMillis();
        try {
            List<Record> allRecords = new ArrayList<>();
            recordRepository.findAll().forEach(allRecords::add);
            cacheService.clearAll();
            cacheService.bulkLoad(allRecords);
            log.info("Cache warm-up complete — {} records in {}ms",
                    allRecords.size(), System.currentTimeMillis() - start);
        } catch (Exception e) {
            // Non-fatal — do NOT rethrow; cache failure must not fail the Batch job
            log.error("Cache warm-up failed (non-fatal): {}", e.getMessage(), e);
        }
    }
}