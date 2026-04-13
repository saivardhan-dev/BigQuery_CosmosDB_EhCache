package com.dataplatform.bq_cdb_ch.service;

import com.dataplatform.bq_cdb_ch.config.EhCacheConfig.RecordList;
import com.dataplatform.bq_cdb_ch.model.Record;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.ehcache.Cache;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Service encapsulating all native EhCache 3 operations.
 *
 * Uses typed {@link RecordList} (extends ArrayList<Record>) for the
 * all-records cache — no unchecked casts anywhere.
 *
 * Cache keys:
 *  - records-by-id : key = record id (String)
 *  - records-all   : key = "ALL"  (single RecordList entry)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EhCacheService {

    private final Cache<String, Record>     recordsByIdCache;
    private final Cache<String, RecordList> recordsAllCache;   // typed — no raw cast
    private final MeterRegistry             meterRegistry;

    private static final String ALL_KEY = "ALL";

    // ── Single-record operations ───────────────────────────────────────────

    public Optional<Record> getById(String id) {
        try {
            Timer.Sample sample = Timer.start(meterRegistry);
            Record record = recordsByIdCache.get(id);
            sample.stop(meterRegistry.timer("ehcache.get",
                    "cache", "records-by-id",
                    "hit", String.valueOf(record != null)));

            if (record != null) {
                log.debug("Cache HIT  records-by-id id={}", id);
                meterRegistry.counter("ehcache.hits", "cache", "records-by-id").increment();
            } else {
                log.debug("Cache MISS records-by-id id={}", id);
                meterRegistry.counter("ehcache.misses", "cache", "records-by-id").increment();
            }
            return Optional.ofNullable(record);
        } catch (Exception e) {
            log.warn("EhCache getById failed for id={}: {}", id, e.getMessage());
            return Optional.empty();
        }
    }

    public void put(Record record) {
        if (record == null || record.getId() == null) return;
        try {
            recordsByIdCache.put(record.getId(), record);
            log.debug("Cache PUT records-by-id id={}", record.getId());
        } catch (Exception e) {
            log.warn("EhCache put failed for id={}: {}", record.getId(), e.getMessage());
        }
    }

    public void evict(String id) {
        try {
            recordsByIdCache.remove(id);
            log.debug("Cache EVICT records-by-id id={}", id);
        } catch (Exception e) {
            log.warn("EhCache evict failed for id={}: {}", id, e.getMessage());
        }
    }

    // ── Bulk / list operations ─────────────────────────────────────────────

    public Optional<List<Record>> getAllRecords() {
        try {
            RecordList records = recordsAllCache.get(ALL_KEY);
            if (records != null) {
                meterRegistry.counter("ehcache.hits", "cache", "records-all").increment();
                log.debug("Cache HIT  records-all ({} entries)", records.size());
            } else {
                meterRegistry.counter("ehcache.misses", "cache", "records-all").increment();
                log.debug("Cache MISS records-all");
            }
            return Optional.ofNullable(records);
        } catch (Exception e) {
            log.warn("EhCache getAllRecords failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Bulk-load all records into both caches.
     * Uses typed RecordList — no unchecked casts.
     * Cache failures are swallowed — Cosmos DB is source of truth.
     */
    public void bulkLoad(List<Record> records) {
        if (records == null || records.isEmpty()) {
            log.warn("bulkLoad called with empty list — skipping.");
            return;
        }
        log.info("EhCache bulk-load started: {} records", records.size());
        long start = System.currentTimeMillis();
        try {
            Map<String, Record> recordMap = new LinkedHashMap<>(records.size());
            for (Record r : records) {
                if (r.getId() != null) recordMap.put(r.getId(), r);
            }
            recordsByIdCache.putAll(recordMap);
            recordsAllCache.put(ALL_KEY, new RecordList(records)); // type-safe
            meterRegistry.gauge("ehcache.size", records.size());
            log.info("EhCache bulk-load complete: {} records in {}ms",
                    records.size(), System.currentTimeMillis() - start);
        } catch (Exception e) {
            log.error("EhCache bulk-load failed (non-fatal): {}", e.getMessage(), e);
        }
    }

    public void clearAll() {
        try {
            recordsByIdCache.clear();
            recordsAllCache.clear();
            log.info("All EhCaches cleared.");
        } catch (Exception e) {
            log.warn("EhCache clearAll failed: {}", e.getMessage());
        }
    }

    public Map<String, Object> getCacheStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        try {
            stats.put("recordsByIdCache.estimatedSize", countEntries(recordsByIdCache));
            RecordList all = recordsAllCache.get(ALL_KEY);
            stats.put("recordsAllCache.hasAll", all != null);
            stats.put("recordsAllCache.listSize", all != null ? all.size() : 0);
        } catch (Exception e) {
            stats.put("error", e.getMessage());
        }
        return stats;
    }

    private long countEntries(Cache<?, ?> cache) {
        long count = 0;
        for (Cache.Entry<?, ?> ignored : cache) count++;
        return count;
    }
}