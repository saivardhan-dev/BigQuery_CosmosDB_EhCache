package com.dataplatform.bq_cdb_ch.model;

import com.azure.spring.data.cosmos.core.mapping.Container;
import com.azure.spring.data.cosmos.core.mapping.PartitionKey;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;

import java.io.Serializable;
import java.time.Instant;

/**
 * Domain entity mapped from the BigQuery CSV schema:
 *   id, name, category, value, updated_at
 *
 * Partition key = id (every document gets its own logical partition,
 * which is the recommended pattern for point-read-heavy workloads
 * and avoids hot partitions when the dataset has no natural high-cardinality key).
 *
 * Annotation sources:
 *  @Id, @Version      → org.springframework.data.annotation  (spring-data-commons)
 *  @Container, @PartitionKey → com.azure.spring.data.cosmos.core.mapping
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@Container(containerName = "records")
public class Record implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * CSV column 1: "rec-0000001", "rec-0000002", …
     * Cosmos document ID AND partition key.
     * Using id as the partition key makes every point-read
     * a single-partition lookup — the most efficient access pattern.
     */
    @Id
    @PartitionKey
    private String id;

    /**
     * CSV column 2: "Product 1", "Product 2", …
     */
    private String name;

    /**
     * CSV column 3: software, hardware, furniture, clothing, food,
     * automotive, sports, books, toys, electronics.
     */
    private String category;

    /**
     * CSV column 4: numeric value (stored as Double for flexibility).
     */
    private Double value;

    /**
     * CSV column 5: "2026-04-01 09:00:00" — stored as UTC Instant.
     */
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant updatedAt;

    /**
     * Set by the pipeline at ingest time — not present in the CSV.
     */
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant syncedAt;

    /**
     * ETag for optimistic concurrency — managed automatically by Spring Data Cosmos.
     */
    @Version
    private String etag;
}