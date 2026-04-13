package com.dataplatform.bq_cdb_ch.scheduler;

import com.dataplatform.bq_cdb_ch.dto.SyncResult;
import com.dataplatform.bq_cdb_ch.service.DataSyncService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scheduled trigger for the automatic data sync pipeline.
 *
 *  1. Startup sync  — runs once automatically when the application is fully
 *                     started (ApplicationReadyEvent). This ensures Cosmos DB
 *                     and EhCache are populated immediately without waiting
 *                     for the first cron fire.
 *
 *  2. Hourly sync   — repeats every 1 hour via cron (configurable via
 *                     scheduler.sync.cron). Keeps data fresh.
 *
 * Controlled by:
 *  - scheduler.sync.enabled=true/false  — enable/disable entirely.
 *  - scheduler.sync.cron                — cron expression (default: every hour).
 *
 * Guards against concurrent execution: if a sync is already running when
 * the next trigger fires, the new trigger is skipped with a warning.
 *
 * Manual trigger also available via POST /api/v1/sync/trigger.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "scheduler.sync.enabled", havingValue = "true", matchIfMissing = true)
public class DataSyncScheduler {

    private final DataSyncService dataSyncService;
    private final MeterRegistry   meterRegistry;

    @Value("${scheduler.sync.run-on-startup:true}")
    private boolean runOnStartup;

    /** Guards against overlapping sync executions. */
    private final AtomicBoolean syncInProgress = new AtomicBoolean(false);

    private volatile Instant    lastSyncAt;
    private volatile SyncResult lastSyncResult;

    // ── Startup sync ───────────────────────────────────────────────────────

    /**
     * Runs once when the application is fully started and ready to serve traffic.
     *
     * ApplicationReadyEvent fires AFTER:
     *   - All beans are initialized (Cosmos DB, EhCache, repositories)
     *   - CosmosContainerInitializer has created the database and container
     *   - The HTTP server is up
     *
     * This means the first sync starts automatically ~9 seconds after boot,
     * without any manual trigger needed.
     *
     * Runs in the main thread — the application will be available to serve
     * requests during the sync since Tomcat is already started.
     * Set scheduler.sync.run-on-startup=false to disable startup sync.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        if (!runOnStartup) {
            log.info("Startup sync disabled (scheduler.sync.run-on-startup=false).");
            return;
        }
        log.info("=== Application ready — triggering startup sync (BigQuery → Cosmos DB → EhCache) ===");
        triggerSync("STARTUP");
    }

    // ── Recurring hourly sync ──────────────────────────────────────────────


//      Fires every 12 hours to keep Cosmos DB and EhCache in sync with BigQuery.
//      Default: midnight and noon  (0 0 */12 * * *)
//      Override via SYNC_CRON env var.
//
//              Note: A full sync of 1M records takes ~2.5 hours at 1000 RU/s.
//      The AtomicBoolean guard ensures the startup sync does not overlap
//      with a scheduled fire


    @Scheduled(cron = "${scheduler.sync.cron:0 0 */12 * * *}")
    public void scheduledSync() {
        log.info("Hourly scheduled sync triggered at {}", Instant.now());
        triggerSync("SCHEDULED");
    }

    // ── Manual trigger (used by SyncController) ────────────────────────────

    public SyncResult triggerManualSync() {
        log.info("Manual sync triggered at {}", Instant.now());
        return triggerSync("MANUAL");
    }

    // ── Status accessors ───────────────────────────────────────────────────

    public boolean isSyncInProgress() { return syncInProgress.get(); }
    public Instant getLastSyncAt()     { return lastSyncAt; }
    public SyncResult getLastSyncResult() { return lastSyncResult; }

    // ── Core sync execution ────────────────────────────────────────────────

    private SyncResult triggerSync(String triggerType) {
        if (!syncInProgress.compareAndSet(false, true)) {
            log.warn("Sync skipped — another sync is already running ({} trigger).", triggerType);
            meterRegistry.counter("sync.skipped", "reason", "already_running").increment();
            return SyncResult.builder()
                    .status(SyncResult.SyncStatus.SKIPPED)
                    .errorMessage("A sync is already in progress. Try again shortly.")
                    .build();
        }

        try {
            meterRegistry.counter("sync.triggered", "type", triggerType).increment();
            SyncResult result = dataSyncService.executeFullSync();
            lastSyncAt     = Instant.now();
            lastSyncResult = result;
            return result;
        } catch (Exception e) {
            log.error("Sync failed unexpectedly ({}): {}", triggerType, e.getMessage(), e);
            meterRegistry.counter("sync.errors", "type", triggerType).increment();
            SyncResult failed = SyncResult.builder()
                    .status(SyncResult.SyncStatus.FAILED)
                    .errorMessage(e.getMessage())
                    .build();
            lastSyncResult = failed;
            return failed;
        } finally {
            syncInProgress.set(false);
        }
    }
}