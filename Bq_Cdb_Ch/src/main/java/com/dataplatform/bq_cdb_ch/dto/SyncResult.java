package com.dataplatform.bq_cdb_ch.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Result DTO returned after a data sync operation.
 * Includes per-step breakdown for observability.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SyncResult {

    public enum SyncStatus {
        SUCCESS, PARTIAL_SUCCESS, FAILED, SKIPPED
    }

    private SyncStatus status;

    private String jobId;

    private long totalReadFromBigQuery;
    private long totalWrittenToCosmos;
    private long totalLoadedIntoCache;
    private long failedRecords;

    private long durationMs;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant startedAt;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant completedAt;

    /** Step-level breakdown for observability. */
    private Map<String, StepResult> steps;

    /** Non-fatal warnings accumulated during sync. */
    private List<String> warnings;

    /** Error message if status is FAILED. */
    private String errorMessage;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StepResult {
        private String stepName;
        private SyncStatus status;
        private long recordsProcessed;
        private long durationMs;
        private String errorMessage;
    }
}