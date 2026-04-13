package com.dataplatform.bq_cdb_ch.controller;

import com.dataplatform.bq_cdb_ch.dto.SyncResult;
import com.dataplatform.bq_cdb_ch.scheduler.DataSyncScheduler;
import com.dataplatform.bq_cdb_ch.service.EhCacheService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST API for managing data sync operations and cache lifecycle.
 *
 * Endpoints:
 *  POST /api/v1/sync/trigger   — manually trigger full sync
 *  GET  /api/v1/sync/status    — last sync result + scheduler state
 *  POST /api/v1/cache/refresh  — evict all and reload cache
 *  POST /api/v1/cache/evict/{id} — evict a single record from cache
 *  GET  /api/v1/cache/stats    — cache diagnostic info
 */
@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Tag(name = "Sync & Cache", description = "Data sync pipeline control and cache management")
public class SyncController {

    private final DataSyncScheduler dataSyncScheduler;
    private final EhCacheService    cacheService;

    // ── Sync endpoints ─────────────────────────────────────────────────────

    @PostMapping("/sync/trigger")
    @Operation(
            summary = "Trigger a full data sync",
            description = "Executes: BigQuery → Cosmos DB → EhCache. Returns 409 if sync already in progress.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Sync completed (SUCCESS or PARTIAL_SUCCESS)"),
                    @ApiResponse(responseCode = "409", description = "Sync already in progress"),
                    @ApiResponse(responseCode = "500", description = "Sync failed")
            }
    )
    public ResponseEntity<SyncResult> triggerSync() {
        if (dataSyncScheduler.isSyncInProgress()) {
            return ResponseEntity.status(409)
                    .body(SyncResult.builder()
                            .status(SyncResult.SyncStatus.SKIPPED)
                            .errorMessage("A sync is already in progress. Try again shortly.")
                            .build());
        }

        log.info("Manual sync triggered via REST API.");
        SyncResult result = dataSyncScheduler.triggerManualSync();

        int httpStatus = switch (result.getStatus()) {
            case SUCCESS, PARTIAL_SUCCESS -> 200;
            case SKIPPED                  -> 409;
            case FAILED                   -> 500;
        };

        return ResponseEntity.status(httpStatus).body(result);
    }

    @GetMapping("/sync/status")
    @Operation(
            summary = "Get sync status",
            description = "Returns the last sync result and whether a sync is currently running."
    )
    public ResponseEntity<Map<String, Object>> getSyncStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("syncInProgress", dataSyncScheduler.isSyncInProgress());
        status.put("lastSyncAt",     dataSyncScheduler.getLastSyncAt());
        status.put("lastSyncResult", dataSyncScheduler.getLastSyncResult());
        return ResponseEntity.ok(status);
    }

    // ── Cache endpoints ────────────────────────────────────────────────────

    @PostMapping("/cache/refresh")
    @Operation(
            summary = "Refresh the EhCache",
            description = "Clears all cache entries. Entries are lazily re-populated on next read, " +
                    "or trigger POST /api/v1/sync/trigger for immediate full reload."
    )
    public ResponseEntity<Map<String, Object>> refreshCache() {
        log.info("Cache refresh requested via REST API.");
        long start = System.currentTimeMillis();
        try {
            cacheService.clearAll();
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "CLEARED");
            response.put("message",
                    "Cache cleared. Entries will be lazily re-populated on next read. " +
                            "Use POST /api/v1/sync/trigger for an immediate full reload.");
            response.put("durationMs", System.currentTimeMillis() - start);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Cache refresh failed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("status", "FAILED", "error", e.getMessage()));
        }
    }

    @PostMapping("/cache/evict/{id}")
    @Operation(
            summary = "Evict a single record from cache",
            description = "Removes one entry from the by-id cache. Next read will fetch from Cosmos DB."
    )
    public ResponseEntity<Map<String, Object>> evictRecord(@PathVariable String id) {
        cacheService.evict(id);
        log.info("Cache evicted for record id={}", id);
        return ResponseEntity.ok(Map.of(
                "status",  "EVICTED",
                "id",      id,
                "message", "Record evicted. Next read will fetch from Cosmos DB."
        ));
    }

    @GetMapping("/cache/stats")
    @Operation(
            summary = "Cache statistics",
            description = "Returns EhCache entry counts and metadata. For observability and debugging."
    )
    public ResponseEntity<Map<String, Object>> cacheStats() {
        return ResponseEntity.ok(cacheService.getCacheStats());
    }
}