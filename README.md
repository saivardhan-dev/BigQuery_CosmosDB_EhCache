# BigQuery → Cosmos DB → EhCache Pipeline

A Spring Boot 3.2 microservice that syncs data from **Google BigQuery** into **Azure Cosmos DB** and serves it via **EhCache** with a REST API. Data is automatically loaded on startup and refreshed every **12 hours** via a scheduler.

---

## Architecture

```
Google BigQuery
      │
      │  (scheduled sync: startup + every hour)
      ▼
Azure Cosmos DB  ←→  Spring Data Cosmos
      │
      │  (cache-aside pattern)
      ▼
EhCache (native, off-heap)
      │
      ▼
REST API  (Spring MVC)
```

**Flow:**
1. On startup, `DataSyncScheduler` triggers a full sync automatically
2. `BigQueryService` fetches all 1M records using the BigQuery Jobs API
3. `DataSyncService` writes records to Cosmos DB in **parallel chunks** (chunk=500, threads=8)
4. Records are bulk-loaded into EhCache after Cosmos write completes (~2.5 hours at 1000 RU/s)
5. Every subsequent API request checks EhCache first (cache-aside), falling back to Cosmos DB on miss
6. Every **12 hours** the full sync repeats automatically

---

## Tech Stack

| Component | Technology |
|---|---|
| Framework | Spring Boot 3.2.4, Java 17 |
| Source | Google BigQuery (`google-cloud-bigquery:2.38.2`) |
| Database | Azure Cosmos DB (`azure-spring-data-cosmos:5.19.0`) |
| Cache | EhCache 3.10.8 (native, no JCache/XML) |
| Batch | Spring Batch 5.1.1 + H2 in-memory job repository |
| Resilience | Resilience4j 2.2.0 (circuit breaker + retry on BigQuery) |
| Observability | Spring Actuator, Micrometer, Prometheus |
| API Docs | SpringDoc OpenAPI / Swagger UI |
| Build | Maven |

---

## Prerequisites

- Java 17 (Amazon Corretto recommended)
- Maven 3.8+
- Google Cloud project with BigQuery dataset and table
- Azure Cosmos DB account (Core SQL API)
- `gcloud auth application-default login` for local BigQuery auth

---

## BigQuery Table Schema

```sql
CREATE TABLE `your-project.cache_dataset.cache_table` (
  id         STRING,
  name       STRING,
  category   STRING,
  value      INTEGER,
  updated_at TIMESTAMP
);
```

---

## Configuration

### Environment Variables (required)

Set these in IntelliJ → Run → Edit Configurations → Environment Variables (use the table editor, one row per variable):

| Variable | Example Value | Description |
|---|---|---|
| `COSMOS_ENDPOINT` | `https://your-account.documents.azure.com:443/` | Cosmos DB account endpoint |
| `COSMOS_KEY` | `0aIVuf...==` | Cosmos DB primary key (Base64, no hyphens) |
| `COSMOS_DATABASE` | `BigQuery-to-CosmosDB` | Database name (auto-created on startup) |
| `BIGQUERY_PROJECT_ID` | `bigquery-to-ehcache` | GCP project ID |
| `BIGQUERY_DATASET` | `cache_dataset` | BigQuery dataset name |
| `BIGQUERY_TABLE` | `cache_table` | BigQuery table name |
| `CACHE_OFFHEAP_MB` | `512` | EhCache off-heap size (use 512 locally, 4096 in prod) |

### VM Options (IntelliJ)

```
-XX:MaxDirectMemorySize=1g
```

### Key `application.yml` Settings

```yaml
batch-pipeline:
  chunk-size:     100    # Records per Cosmos saveAll() call
  throttle-limit: 4      # Parallel write threads

# Tune for your Cosmos RU/s provisioning:
# 400 RU/s  → chunk=100,  threads=4   → ~5-6 hours for 1M records
# 1000 RU/s → chunk=100,  threads=4   → ~2.5 hours for 1M records
# 4000 RU/s → chunk=300,  threads=12  → ~40 minutes for 1M records
# 10000 RU/s → chunk=500, threads=16  → ~15 minutes for 1M records
```

---

## Running Locally

```bash
# 1. Clone and build
git clone <repo>
cd Bq_Cdb_Ch
mvn clean package -DskipTests

# 2. Set environment variables (see above)

# 3. Run
mvn spring-boot:run
# or run BqCdbChApplication.java from IntelliJ
```

On startup the application will:
1. Create the Cosmos DB database and `records` container automatically
2. Trigger a full BigQuery → Cosmos DB → EhCache sync
3. Start serving requests on `http://localhost:8080`

---

## API Endpoints

### Records

| Method | URL | Description |
|---|---|---|
| `GET` | `/api/v1/records` | Paginated list of all records |
| `GET` | `/api/v1/records/{id}` | Get record by ID (cache-aside) |
| `GET` | `/api/v1/records/category/{category}` | Records by category |
| `GET` | `/api/v1/records/search/above-value?threshold=50000` | Records above value threshold |
| `DELETE` | `/api/v1/records/{id}` | Delete a record |

### Sync & Cache

| Method | URL | Description |
|---|---|---|
| `POST` | `/api/v1/sync/trigger` | Manually trigger a full sync |
| `GET` | `/api/v1/sync/status` | Last sync result and status |
| `POST` | `/api/v1/cache/refresh` | Clear EhCache and reload from Cosmos DB |
| `POST` | `/api/v1/cache/evict/{id}` | Evict a single record from cache |

### Actuator

| URL | Description |
|---|---|
| `/actuator/health` | Health check (includes Cosmos DB + circuit breaker) |
| `/actuator/metrics` | All Micrometer metrics |
| `/actuator/prometheus` | Prometheus scrape endpoint |
| `/actuator/caches` | EhCache statistics |
| `/actuator/batch` | Spring Batch job execution history |

### API Documentation

```
http://localhost:8080/swagger-ui.html
http://localhost:8080/api-docs
```

---

## Useful Cosmos DB Queries (Data Explorer)

```sql
-- Count total records
SELECT VALUE COUNT(1) FROM c

-- Get a specific record
SELECT * FROM c WHERE c.id = 'rec-0000001'

-- Records by category
SELECT * FROM c WHERE c.category = 'software'

-- Records above value
SELECT * FROM c WHERE c.value > 50000

-- Count by category
SELECT c.category, COUNT(1) as count FROM c GROUP BY c.category

-- Most recently synced
SELECT TOP 10 c.id, c.syncedAt FROM c ORDER BY c.syncedAt DESC
```

---

## Sync Performance Guide

The initial load of 1M records depends on Cosmos DB provisioned throughput (RU/s).

To speed up the **initial load only**, temporarily increase RU/s in Azure Portal:
> Cosmos DB → Data Explorer → `records` container → **Scale & Settings** → increase RU/s → Save

After the sync completes, scale back down.

| RU/s | chunk-size | throttle-limit | ~Time for 1M records |
|---|---|---|---|
| 400 | 100 | 4 | ~8+ hours |
| 1000 | 500 | 8 | ~2.5 hours ✓ (observed) |
| 4000 | 500 | 8 | ~40-50 minutes |
| 10000 | 500 | 16 | ~15-20 minutes |

> **Current config:** 1000 RU/s, chunk=500, threads=8 → **~2.5 hours** for 1M records (verified).

> **Scheduler:** Full sync runs on startup + every **12 hours** (cron: `0 0 */12 * * *`). Since a full sync takes ~2.5 hours, the 12-hour interval ensures the previous sync is always complete before the next one starts. The `AtomicBoolean` concurrency guard prevents overlapping syncs regardless.

---

## Project Structure

```
src/main/java/com/dataplatform/bq_cdb_ch/
├── BqCdbChApplication.java
├── batch/
│   ├── BatchConfig.java              # Spring Batch job definition
│   ├── BigQueryRecordsReader.java    # ItemReader: reads from BigQuery
│   └── CosmosRecordsWriter.java      # ItemWriter: writes to Cosmos DB
├── config/
│   ├── BigQueryConfig.java           # BigQuery client (ADC or service account)
│   ├── CosmosDbConfig.java           # Cosmos DB client + Spring Data config
│   ├── CosmosContainerInitializer.java # Auto-creates DB + container on startup
│   ├── CosmosHealthIndicator.java    # Custom health check
│   ├── EhCacheConfig.java            # Native EhCache (programmatic, no XML)
│   ├── GlobalExceptionHandler.java   # RFC 7807 ProblemDetail error responses
│   ├── OpenApiConfig.java            # Swagger UI config
│   └── RecordNotFoundException.java  # Custom exception
├── controller/
│   ├── RecordController.java         # GET/DELETE /api/v1/records/**
│   └── SyncController.java           # POST /api/v1/sync/**, cache endpoints
├── dto/
│   ├── PagedResponse.java            # Generic paginated response wrapper
│   └── SyncResult.java               # Sync job result with per-step breakdown
├── filter/
│   └── CorrelationIdFilter.java      # X-Correlation-Id header + MDC
├── model/
│   └── Record.java                   # Cosmos DB document model
├── repository/
│   └── RecordRepository.java         # Spring Data Cosmos repository
├── scheduler/
│   └── DataSyncScheduler.java        # Startup sync + hourly cron
└── service/
    ├── BigQueryService.java          # BQ fetch with circuit breaker + retry
    ├── DataSyncService.java          # Pipeline orchestration (parallel chunks)
    ├── EhCacheService.java           # Cache get/put/evict/bulkLoad
    └── RecordsService.java           # Cache-aside read logic
```

---

## Resilience

### Circuit Breaker (BigQuery)
- **CLOSED** → normal operation
- **OPEN** → fast-fail for 30s after 50% failure rate over 10 calls
- **HALF-OPEN** → 3 trial calls to decide recovery
- Fallback returns empty list → sync aborts cleanly without writing empty data to Cosmos

### Retry (BigQuery)
- Up to 3 attempts with exponential backoff: 2s → 4s → 8s
- Fires **before** the circuit breaker records a failure (transient errors don't trip the breaker)

### Cosmos Write Retry
- Each chunk retried up to 3 times with linear backoff on failure
- Failed chunks are counted and reported in `SyncResult` without stopping the pipeline

---
