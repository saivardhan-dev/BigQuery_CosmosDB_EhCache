package com.dataplatform.bq_cdb_ch.batch;

import com.dataplatform.bq_cdb_ch.model.Record;
import com.dataplatform.bq_cdb_ch.service.EhCacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Instant;

/**
 * Spring Batch configuration for the BigQuery → Cosmos DB ingestion pipeline.
 *
 * Job: dataPipelineJob
 *   Step 1 bigQueryToCosmosStep — chunk-oriented: read BigQuery, write to Cosmos.
 *   Step 2 cacheWarmUpStep      — tasklet: load all Cosmos records into EhCache.
 *
 * FIX: Removed getPartitionKey() / setPartitionKey() calls from the processor.
 *      The Record model uses id as the @PartitionKey — no separate partitionKey
 *      field exists. The processor now only validates that id is non-blank
 *      and stamps syncedAt.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final JobRepository              jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final BigQueryRecordsReader      bigQueryRecordsReader;
    private final CosmosRecordsWriter        cosmosRecordsWriter;
    private final EhCacheService             cacheService;

    @Value("${batch-pipeline.chunk-size:500}")
    private int chunkSize;

    // ── Job ────────────────────────────────────────────────────────────────

    @Bean
    public Job dataPipelineJob() {
        return new JobBuilder("dataPipelineJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(jobExecutionListener())
                .start(bigQueryToCosmosStep())
                .next(cacheWarmUpStep())
                .build();
    }

    // ── Steps ──────────────────────────────────────────────────────────────

    @Bean
    public Step bigQueryToCosmosStep() {
        return new StepBuilder("bigQueryToCosmosStep", jobRepository)
                .<Record, Record>chunk(chunkSize, transactionManager)
                .reader(bigQueryRecordsReader)
                .processor(recordProcessor())
                .writer(cosmosRecordsWriter)
                .faultTolerant()
                .retryLimit(3)
                .retry(Exception.class)
                .skipLimit(100)
                .skip(IllegalArgumentException.class)
                .listener(stepExecutionListener())
                .build();
    }

    @Bean
    public Step cacheWarmUpStep() {
        return new StepBuilder("cacheWarmUpStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    log.info("Cache warm-up step: loading all records from Cosmos into EhCache");
                    try {
                        cosmosRecordsWriter.warmUpCache(cacheService);
                        contribution.incrementWriteCount(1);
                    } catch (Exception e) {
                        // Non-fatal — cache failure must not fail the job
                        log.error("Cache warm-up failed (non-fatal): {}", e.getMessage(), e);
                    }
                    return org.springframework.batch.repeat.RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    // ── Processor ─────────────────────────────────────────────────────────

    /**
     * Validates required fields and stamps syncedAt.
     *
     * FIX: No partitionKey field on Record — id IS the partition key (@Id @PartitionKey).
     * Validation only checks that id is non-blank. Returns null to skip invalid records
     * (Spring Batch counts them as skipped, within the skipLimit configured above).
     */
    @Bean
    public ItemProcessor<Record, Record> recordProcessor() {
        return record -> {
            if (record == null) {
                log.warn("Processor received null record — skipping");
                return null;
            }
            if (record.getId() == null || record.getId().isBlank()) {
                log.warn("Skipping record with null/blank id");
                return null;
            }
            // Stamp the sync timestamp — id is already the partition key
            record.setSyncedAt(Instant.now());
            return record;
        };
    }

    // ── Listeners ─────────────────────────────────────────────────────────

    @Bean
    public JobExecutionListener jobExecutionListener() {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                log.info(">>> Job '{}' starting | JobId={}",
                        jobExecution.getJobInstance().getJobName(), jobExecution.getJobId());
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                BatchStatus status = jobExecution.getStatus();
                long duration = (jobExecution.getEndTime() != null && jobExecution.getStartTime() != null)
                        ? java.time.Duration.between(
                        jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis()
                        : -1L;
                log.info("<<< Job '{}' finished | Status={} | Duration={}ms | JobId={}",
                        jobExecution.getJobInstance().getJobName(),
                        status, duration, jobExecution.getJobId());
                if (status == BatchStatus.FAILED) {
                    jobExecution.getAllFailureExceptions()
                            .forEach(ex -> log.error("Job failure: {}", ex.getMessage(), ex));
                }
            }
        };
    }

    @Bean
    public StepExecutionListener stepExecutionListener() {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                log.info("  Step '{}' starting", stepExecution.getStepName());
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                log.info("  Step '{}' finished | Status={} | Read={} Written={} Skipped={}",
                        stepExecution.getStepName(), stepExecution.getStatus(),
                        stepExecution.getReadCount(), stepExecution.getWriteCount(),
                        stepExecution.getSkipCount());
                return stepExecution.getExitStatus();
            }
        };
    }
}