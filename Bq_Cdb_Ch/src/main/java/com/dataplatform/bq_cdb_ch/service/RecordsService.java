package com.dataplatform.bq_cdb_ch.service;

import com.dataplatform.bq_cdb_ch.dto.PagedResponse;
import com.dataplatform.bq_cdb_ch.model.Record;
import com.dataplatform.bq_cdb_ch.repository.RecordRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Core service for serving Record data via strict cache-aside pattern.
 *
 * Record fields aligned to CSV schema: id, name, category, value, updatedAt, syncedAt.
 * Partition key is category (10 distinct values from CSV data).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RecordsService {

    private final RecordRepository recordRepository;
    private final EhCacheService   cacheService;
    private final MeterRegistry    meterRegistry;

    // ── Single record lookup ───────────────────────────────────────────────

    public Optional<Record> getById(String id) {
        Optional<Record> cached = cacheService.getById(id);
        if (cached.isPresent()) {
            meterRegistry.counter("api.records.source", "source", "cache").increment();
            return cached;
        }
        log.debug("Cache miss id={}; querying Cosmos DB", id);
        Optional<Record> fromDb = recordRepository.findById(id);
        fromDb.ifPresent(r -> {
            cacheService.put(r);
            meterRegistry.counter("api.records.source", "source", "db").increment();
        });
        return fromDb;
    }

    /** Point lookup by id + category — avoids cross-partition scan. */
    public Optional<Record> getByIdAndCategory(String id, String category) {
        Optional<Record> cached = cacheService.getById(id);
        if (cached.isPresent() && category.equals(cached.get().getCategory())) {
            meterRegistry.counter("api.records.source", "source", "cache").increment();
            return cached;
        }
        Optional<Record> fromDb = recordRepository.findByIdAndCategory(id, category);
        fromDb.ifPresent(cacheService::put);
        return fromDb;
    }

    // ── Paged list queries ─────────────────────────────────────────────────

    /** All records paginated — cache first, then Cosmos. */
    public PagedResponse<Record> getAllPaged(int page, int size) {
        Optional<List<Record>> allCached = cacheService.getAllRecords();
        if (allCached.isPresent()) {
            List<Record> all = allCached.get();
            int from = Math.min(page * size, all.size());
            int to   = Math.min(from + size, all.size());
            meterRegistry.counter("api.records.source", "source", "cache").increment();
            return PagedResponse.of(all.subList(from, to), page, size, all.size(), "cache");
        }
        Pageable pageable = PageRequest.of(page, size, Sort.by("updatedAt").descending());
        Page<Record> cosmosPage = recordRepository.findAll(pageable);
        meterRegistry.counter("api.records.source", "source", "db").increment();
        return PagedResponse.of(
                cosmosPage.getContent(), page, size,
                cosmosPage.getTotalElements(), "database");
    }

    /** Records filtered by category (partition-key query — no fan-out). */
    public PagedResponse<Record> getByCategory(String category, int page, int size) {
        Pageable pageable = PageRequest.of(page, size, Sort.by("updatedAt").descending());
        Slice<Record> slice = recordRepository.findByCategory(category, pageable);
        return PagedResponse.of(slice.getContent(), page, size, -1L, "database");
    }

    /** Records above a value threshold. */
    public List<Record> getAboveValue(double threshold) {
        return recordRepository.findRecordsAboveValue(threshold);
    }

    /** Records in a category above a value threshold. */
    public List<Record> getByCategoryAboveValue(String category, double threshold) {
        return recordRepository.findByCategoryAndValueAbove(category, threshold);
    }

    // ── Write operations ───────────────────────────────────────────────────

    public Record upsert(Record record) {
        Record saved = recordRepository.save(record);
        cacheService.put(saved);
        cacheService.clearAll();
        log.info("Upserted record id={}", saved.getId());
        return saved;
    }

    public void delete(String id) {
        recordRepository.deleteById(id);
        cacheService.evict(id);
        cacheService.clearAll();
        log.info("Deleted record id={}", id);
    }
}