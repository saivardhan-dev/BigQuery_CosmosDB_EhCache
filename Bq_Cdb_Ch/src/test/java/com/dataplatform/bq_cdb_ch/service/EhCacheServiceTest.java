package com.dataplatform.bq_cdb_ch.service;

import com.dataplatform.bq_cdb_ch.config.EhCacheConfig;
import com.dataplatform.bq_cdb_ch.config.EhCacheConfig.RecordList;
import com.dataplatform.bq_cdb_ch.model.Record;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure unit tests for EhCacheService — no Spring context, fast startup.
 *
 * FIX: ResourcePoolsBuilder.heap() is an INSTANCE method, not static.
 *      Must call ResourcePoolsBuilder.newResourcePoolsBuilder().heap(...)
 *      instead of ResourcePoolsBuilder.heap(...) directly.
 */
class EhCacheServiceTest {

    private static CacheManager              cacheManager;
    private static Cache<String, Record>     recordsByIdCache;
    private static Cache<String, RecordList> recordsAllCache;

    private EhCacheService cacheService;

    @BeforeAll
    static void setUpCacheManager() {
        cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
                .withCache(
                        EhCacheConfig.CACHE_RECORDS_BY_ID,
                        CacheConfigurationBuilder.newCacheConfigurationBuilder(
                                        String.class,
                                        Record.class,
                                        // FIX: call .newResourcePoolsBuilder() first, then .heap()
                                        ResourcePoolsBuilder.newResourcePoolsBuilder()
                                                .heap(1000, EntryUnit.ENTRIES))
                                .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(
                                        java.time.Duration.ofMinutes(5)))
                                .build())
                .withCache(
                        EhCacheConfig.CACHE_RECORDS_ALL,
                        CacheConfigurationBuilder.newCacheConfigurationBuilder(
                                        String.class,
                                        RecordList.class,
                                        // FIX: same here
                                        ResourcePoolsBuilder.newResourcePoolsBuilder()
                                                .heap(10, EntryUnit.ENTRIES))
                                .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(
                                        java.time.Duration.ofMinutes(5)))
                                .build())
                .build(true);

        recordsByIdCache = cacheManager.getCache(
                EhCacheConfig.CACHE_RECORDS_BY_ID, String.class, Record.class);
        recordsAllCache  = cacheManager.getCache(
                EhCacheConfig.CACHE_RECORDS_ALL, String.class, RecordList.class);
    }

    @AfterAll
    static void tearDown() {
        if (cacheManager != null) cacheManager.close();
    }

    @BeforeEach
    void setUp() {
        recordsByIdCache.clear();
        recordsAllCache.clear();
        cacheService = new EhCacheService(
                recordsByIdCache, recordsAllCache, new SimpleMeterRegistry());
    }

    // ── getById ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("getById returns empty on cache miss")
    void getById_miss() {
        assertThat(cacheService.getById("nonexistent")).isEmpty();
    }

    @Test
    @DisplayName("put then getById returns the record")
    void put_getById() {
        cacheService.put(sample("rec-001", "electronics"));
        Optional<Record> found = cacheService.getById("rec-001");
        assertThat(found).isPresent();
        assertThat(found.get().getId()).isEqualTo("rec-001");
        assertThat(found.get().getCategory()).isEqualTo("electronics");
    }

    // ── evict ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("evict removes record from cache")
    void evict_removes() {
        cacheService.put(sample("rec-002", "software"));
        cacheService.evict("rec-002");
        assertThat(cacheService.getById("rec-002")).isEmpty();
    }

    @Test
    @DisplayName("evict non-existent key does not throw")
    void evict_nonExistent() {
        Assertions.assertDoesNotThrow(() -> cacheService.evict("ghost-id"));
    }

    // ── bulkLoad ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("bulkLoad populates by-id cache for every record")
    void bulkLoad_byId() {
        cacheService.bulkLoad(List.of(
                sample("rec-010", "software"),
                sample("rec-011", "hardware"),
                sample("rec-012", "toys")));
        assertThat(cacheService.getById("rec-010")).isPresent();
        assertThat(cacheService.getById("rec-011")).isPresent();
        assertThat(cacheService.getById("rec-012")).isPresent();
    }

    @Test
    @DisplayName("bulkLoad populates the all-records cache")
    void bulkLoad_allRecords() {
        cacheService.bulkLoad(List.of(
                sample("rec-020", "food"),
                sample("rec-021", "books")));
        Optional<List<Record>> all = cacheService.getAllRecords();
        assertThat(all).isPresent();
        assertThat(all.get()).hasSize(2);
    }

    @Test
    @DisplayName("bulkLoad with empty list is a no-op")
    void bulkLoad_empty() {
        Assertions.assertDoesNotThrow(() -> cacheService.bulkLoad(List.of()));
        assertThat(cacheService.getAllRecords()).isEmpty();
    }

    @Test
    @DisplayName("bulkLoad with null is a no-op")
    void bulkLoad_null() {
        Assertions.assertDoesNotThrow(() -> cacheService.bulkLoad(null));
    }

    // ── getAllRecords ──────────────────────────────────────────────────────

    @Test
    @DisplayName("getAllRecords returns empty when cache is cold")
    void getAllRecords_cold() {
        assertThat(cacheService.getAllRecords()).isEmpty();
    }

    @Test
    @DisplayName("getAllRecords returns full list after bulkLoad")
    void getAllRecords_afterBulkLoad() {
        cacheService.bulkLoad(List.of(
                sample("rec-030", "clothing"),
                sample("rec-031", "furniture"),
                sample("rec-032", "automotive")));
        Optional<List<Record>> result = cacheService.getAllRecords();
        assertThat(result).isPresent();
        assertThat(result.get()).hasSize(3);
    }

    // ── clearAll ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("clearAll empties both caches")
    void clearAll_emptiesBoth() {
        cacheService.bulkLoad(List.of(
                sample("rec-040", "sports"),
                sample("rec-041", "electronics")));
        cacheService.clearAll();
        assertThat(cacheService.getById("rec-040")).isEmpty();
        assertThat(cacheService.getAllRecords()).isEmpty();
    }

    // ── edge cases ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("put with null record does not throw")
    void put_null() {
        Assertions.assertDoesNotThrow(() -> cacheService.put(null));
    }

    @Test
    @DisplayName("put with record missing id does not throw")
    void put_noId() {
        Record noId = Record.builder().category("software").name("no-id").build();
        Assertions.assertDoesNotThrow(() -> cacheService.put(noId));
    }

    // ── getCacheStats ──────────────────────────────────────────────────────

    @Test
    @DisplayName("getCacheStats reflects correct sizes after bulkLoad")
    void cacheStats_correct() {
        cacheService.bulkLoad(List.of(
                sample("rec-050", "software"),
                sample("rec-051", "hardware"),
                sample("rec-052", "food")));
        Map<String, Object> stats = cacheService.getCacheStats();
        assertThat(stats).containsKey("recordsByIdCache.estimatedSize");
        assertThat(stats.get("recordsAllCache.hasAll")).isEqualTo(true);
        assertThat(stats.get("recordsAllCache.listSize")).isEqualTo(3);
    }

    // ── helper ─────────────────────────────────────────────────────────────

    private static Record sample(String id, String category) {
        return Record.builder()
                .id(id)
                .category(category)
                .name("Product " + id)
                .value(50000.0)
                .updatedAt(Instant.parse("2026-04-01T09:00:00Z"))
                .syncedAt(Instant.now())
                .build();
    }
}