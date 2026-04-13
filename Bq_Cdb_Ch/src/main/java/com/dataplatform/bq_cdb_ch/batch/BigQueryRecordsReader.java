package com.dataplatform.bq_cdb_ch.batch;

import com.dataplatform.bq_cdb_ch.model.Record;
import com.dataplatform.bq_cdb_ch.service.BigQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.List;

/**
 * Spring Batch {@link ItemReader} that fetches all records from Google BigQuery.
 *
 * Behaviour:
 *  - First {@link #read()} call → executes the BigQuery query and loads results into memory.
 *  - Subsequent calls → iterates over the in-memory list one record at a time.
 *  - Returns {@code null} when exhausted (signals end-of-data to Spring Batch).
 *
 * Threading: single-threaded by default. For partitioned parallel steps,
 * wrap with SynchronizedItemStreamReader or use a partitioned approach.
 *
 * Memory note: For very large datasets (millions of rows), replace with a
 * streaming cursor using the BigQuery Storage Read API.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BigQueryRecordsReader implements ItemReader<Record> {

    private final BigQueryService bigQueryService;

    private Iterator<Record> iterator;
    private boolean          initialized = false;
    private int              totalFetched = 0;

    /**
     * Returns the next {@link Record} from BigQuery, or {@code null} if exhausted.
     * Initializes the data source lazily on the first call.
     */
    @Override
    public Record read()
            throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {

        if (!initialized) {
            initialize();
        }

        if (iterator != null && iterator.hasNext()) {
            return iterator.next();
        }

        log.debug("BigQueryRecordsReader exhausted after {} records.", totalFetched);
        return null; // signals end-of-input to Spring Batch
    }

    /**
     * Resets state so the reader can be reused across job re-runs in the same JVM.
     */
    public void reset() {
        this.iterator    = null;
        this.initialized = false;
        this.totalFetched = 0;
        log.debug("BigQueryRecordsReader reset.");
    }

    // ── Private ────────────────────────────────────────────────────────────

    private void initialize() {
        log.info("BigQueryRecordsReader: initializing — fetching all records from BigQuery...");
        try {
            List<Record> records = bigQueryService.fetchAllRecords();
            totalFetched = records.size();
            iterator     = records.iterator();
            initialized  = true;
            log.info("BigQueryRecordsReader: initialized with {} records.", totalFetched);
        } catch (Exception e) {
            log.error("BigQueryRecordsReader: initialization failed: {}", e.getMessage(), e);
            throw new NonTransientResourceException("Failed to fetch records from BigQuery", e) {};
        }
    }
}