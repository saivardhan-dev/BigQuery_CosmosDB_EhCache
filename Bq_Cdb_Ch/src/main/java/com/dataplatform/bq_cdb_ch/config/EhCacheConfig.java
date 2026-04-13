package com.dataplatform.bq_cdb_ch.config;

import com.dataplatform.bq_cdb_ch.model.Record;
import lombok.extern.slf4j.Slf4j;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Native EhCache 3 configuration — 100% programmatic, zero XML.
 *
 * FIX: Removed all raw/unchecked generic casts that caused
 *      "uses unchecked or unsafe operations" compiler warnings.
 *
 *      The records-all cache stores a typed wrapper class
 *      {@link RecordList} instead of raw {@code List<Record>},
 *      which eliminates all unchecked cast warnings while keeping
 *      the full native EhCache API.
 *
 * Constraints strictly enforced:
 *  - NO Spring Cache abstraction (@Cacheable / @EnableCaching)
 *  - NO JCache (javax.cache / JSR-107)
 *  - NO ehcache.xml
 */
@Slf4j
@Configuration
public class EhCacheConfig {

    @Value("${ehcache.records.heap-entries:50000}")
    private long heapEntries;

    @Value("${ehcache.records.offheap-mb:256}")
    private long offHeapMb;

    @Value("${ehcache.records.ttl-minutes:30}")
    private long ttlMinutes;

    /** Cache alias constants used throughout the application. */
    public static final String CACHE_RECORDS_BY_ID = "records-by-id";
    public static final String CACHE_RECORDS_ALL   = "records-all";

    /**
     * Typed wrapper that replaces raw {@code List<Record>} as the value type
     * for the records-all cache. Eliminates all unchecked cast compiler warnings.
     * Implements Serializable for EhCache off-heap serialization.
     */
    public static class RecordList extends ArrayList<Record> {
        public RecordList() { super(); }
        public RecordList(List<Record> records) { super(records); }
    }

    /**
     * Singleton EhCache {@link CacheManager}.
     * Registers all caches at startup; destroyed cleanly on shutdown via destroyMethod.
     */
    @Bean(destroyMethod = "close")
    public CacheManager ehCacheManager() {
        log.info("Building native EhCache CacheManager: heap={} entries, offheap={}MB, TTL={}min",
                heapEntries, offHeapMb, ttlMinutes);

        CacheManager manager = CacheManagerBuilder
                .newCacheManagerBuilder()

                // Cache 1: individual Record lookups  (String → Record)
                .withCache(
                        CACHE_RECORDS_BY_ID,
                        CacheConfigurationBuilder
                                .newCacheConfigurationBuilder(
                                        String.class,
                                        Record.class,
                                        ResourcePoolsBuilder.newResourcePoolsBuilder()
                                                .heap(heapEntries, EntryUnit.ENTRIES)
                                                .offheap(offHeapMb, MemoryUnit.MB))
                                .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(
                                        Duration.ofMinutes(ttlMinutes)))
                                .build()
                )

                // Cache 2: full record list (String → RecordList)
                // Uses typed RecordList wrapper — NO unchecked casts needed
                .withCache(
                        CACHE_RECORDS_ALL,
                        CacheConfigurationBuilder
                                .newCacheConfigurationBuilder(
                                        String.class,
                                        RecordList.class,
                                        ResourcePoolsBuilder.newResourcePoolsBuilder()
                                                .heap(10, EntryUnit.ENTRIES)
                                                .offheap(offHeapMb / 2, MemoryUnit.MB))
                                .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(
                                        Duration.ofMinutes(ttlMinutes)))
                                .build()
                )
                .build(true);

        log.info("Native EhCache CacheManager initialised with caches: [{}], [{}]",
                CACHE_RECORDS_BY_ID, CACHE_RECORDS_ALL);
        return manager;
    }

    /**
     * Typed cache bean for single-record lookups.
     * Injected directly into {@link com.dataplatform.bq_cdb_ch.service.EhCacheService}.
     */
    @Bean
    public Cache<String, Record> recordsByIdCache(CacheManager ehCacheManager) {
        return ehCacheManager.getCache(CACHE_RECORDS_BY_ID, String.class, Record.class);
    }

    /**
     * Typed cache bean for the full records list.
     * Uses {@link RecordList} — fully type-safe, no unchecked casts.
     */
    @Bean
    public Cache<String, RecordList> recordsAllCache(CacheManager ehCacheManager) {
        return ehCacheManager.getCache(CACHE_RECORDS_ALL, String.class, RecordList.class);
    }
}