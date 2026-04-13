package com.dataplatform.bq_cdb_ch.controller;

import com.dataplatform.bq_cdb_ch.dto.PagedResponse;
import com.dataplatform.bq_cdb_ch.model.Record;
import com.dataplatform.bq_cdb_ch.service.EhCacheService;
import com.dataplatform.bq_cdb_ch.service.RecordsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST API for querying Records.
 *
 * Record schema (from CSV):
 *   id, name, category, value, updatedAt
 *
 * Categories (10 from CSV):
 *   software, hardware, furniture, clothing, food,
 *   automotive, sports, books, toys, electronics
 */
@Slf4j
@Validated
@RestController
@RequestMapping("/api/v1/records")
@RequiredArgsConstructor
@Tag(name = "Records", description = "Query product records — EhCache → Cosmos DB (cache-aside)")
public class RecordController {

    private final RecordsService recordsService;
    private final EhCacheService cacheService;

    @GetMapping
    @Operation(summary = "List all records (paginated)",
            description = "Served from EhCache if warm; falls back to Cosmos DB.")
    public ResponseEntity<PagedResponse<Record>> getAllRecords(
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) int size) {
        return ResponseEntity.ok(recordsService.getAllPaged(page, size));
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get a record by ID (cache-aside)",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Found"),
                    @ApiResponse(responseCode = "404", description = "Not found")
            })
    public ResponseEntity<Record> getById(@PathVariable @NotBlank String id) {
        return recordsService.getById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/{id}/category/{category}")
    @Operation(summary = "Get by ID + category (partition-key lookup — no cross-partition fan-out)")
    public ResponseEntity<Record> getByIdAndCategory(
            @PathVariable @NotBlank String id,
            @PathVariable @NotBlank String category) {
        return recordsService.getByIdAndCategory(id, category)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/category/{category}")
    @Operation(summary = "Get records by category (paginated)",
            description = "Categories from data: software, hardware, furniture, clothing, " +
                    "food, automotive, sports, books, toys, electronics")
    public ResponseEntity<PagedResponse<Record>> getByCategory(
            @PathVariable @NotBlank String category,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) int size) {
        return ResponseEntity.ok(recordsService.getByCategory(category, page, size));
    }

    @GetMapping("/search/above-value")
    @Operation(summary = "Get records with value above a threshold",
            description = "Value range in the dataset: ~475 to ~99853")
    public ResponseEntity<List<Record>> getAboveValue(
            @Parameter(description = "Minimum value threshold", required = true)
            @RequestParam double threshold) {
        return ResponseEntity.ok(recordsService.getAboveValue(threshold));
    }

    @GetMapping("/search/category-above-value")
    @Operation(summary = "Get records in a category above a value threshold")
    public ResponseEntity<List<Record>> getByCategoryAboveValue(
            @RequestParam @NotBlank String category,
            @RequestParam double threshold) {
        return ResponseEntity.ok(recordsService.getByCategoryAboveValue(category, threshold));
    }

    @GetMapping("/cache/stats")
    @Operation(summary = "EhCache diagnostics (entry counts, list size)")
    public ResponseEntity<Map<String, Object>> cacheStats() {
        return ResponseEntity.ok(cacheService.getCacheStats());
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Delete a record — removes from Cosmos DB and evicts from cache",
            responses = {@ApiResponse(responseCode = "204", description = "Deleted")})
    public ResponseEntity<Void> delete(@PathVariable @NotBlank String id) {
        recordsService.delete(id);
        return ResponseEntity.noContent().build();
    }
}