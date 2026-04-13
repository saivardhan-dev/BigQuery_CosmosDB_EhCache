package com.dataplatform.bq_cdb_ch.repository;

import com.azure.spring.data.cosmos.repository.CosmosRepository;
import com.azure.spring.data.cosmos.repository.Query;
import com.dataplatform.bq_cdb_ch.model.Record;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data Cosmos repository for {@link Record}.
 *
 * Query methods aligned to the actual CSV schema fields:
 *   id, name, category, value, updatedAt, syncedAt
 *
 * Note: category is the partition key — queries that include category
 * in the filter avoid cross-partition fan-out.
 */
@Repository
public interface RecordRepository extends CosmosRepository<Record, String> {

    /** All records in a partition (category). Most efficient query. */
    List<Record> findByCategory(String category);

    /** Paginated records in a category. */
    Slice<Record> findByCategory(String category, Pageable pageable);

    /** Point lookup by ID + category (avoids cross-partition scan). */
    Optional<Record> findByIdAndCategory(String id, String category);

    /** All records with value above a threshold (active pipeline query). */
    @Query("SELECT * FROM c WHERE c.value >= @threshold")
    List<Record> findRecordsAboveValue(@Param("threshold") double threshold);

    /** Records in a category above a value threshold. */
    @Query("SELECT * FROM c WHERE c.category = @category AND c.value >= @threshold")
    List<Record> findByCategoryAndValueAbove(
            @Param("category") String category,
            @Param("threshold") double threshold);

    /** Count records per category — useful for stats endpoint. */
    long countByCategory(String category);
}