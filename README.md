# 🪙 Enterprise Crypto Hybrid Data Engine (Batch & Real-Time)

[![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-0078D4?style=for-the-badge&logo=azure-data-factory&logoColor=white)](https://azure.microsoft.com/en-us/products/data-factory)
[![Azure Logic Apps](https://img.shields.io/badge/Azure%20Logic%20Apps-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/en-us/products/logic-apps)
[![Azure Event Hubs](https://img.shields.io/badge/Azure%20Event%20Hubs-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/en-us/products/event-hubs)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Azure Storage](https://img.shields.io/badge/Azure%20Data%20Lake-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/en-us/products/storage/data-lake-storage/)

---

## 📌 Executive Summary & Hybrid Strategy

This project delivers an end-to-end data engineering platform on **Microsoft Azure** designed to capture both historical trade archives and high-frequency live market signals for cryptocurrency pairs (focusing on BTC/USDT).

### 🎯 Key Engineering Goals
* **Dual-Ingestion Pipeline**: Integrates high-volume **Batch historical backfills** with low-latency **Real-Time price streaming** within a unified cloud architecture.
* **Cost-Optimized Ingestion**: Utilizes serverless orchestration (Azure Logic Apps & Data Factory) to eliminate continuous compute costs while fetching compressed raw files and REST API payloads.
* **Medallion Architecture Foundation**: Raw historical zips and live JSON streams land directly into the **Bronze Storage Layer**, forming an immutable landing zone ready for PySpark/Databricks transformation.

---

## ⚙️ Data Ingestion Layer (Azure Data Factory & Logic Apps)

### 1. Batch Historical Flow (Azure Data Factory)
* **Public Source**: Connects to the public **Binance Vision** HTTP file repository (`https://data.binance.vision/`).
* **Orchestration**: The ADF pipeline `pl_ingest_crypto` executes an HTTP `GET` request using the `month_param` dataset.
* **Data Landing**: Streams raw compressed `.zip` payloads directly into **Azure Data Lake Storage Gen2** (`azcriptoprojectstorage`) under `bronze/historical_crypto_data` using a **Flatten Hierarchy** copy strategy.

#### Pipeline Configuration & Verification
* **Http Linked Service (`Httpcryptoproject`)**: Configured targeting Binance Vision using `AutoResolveIntegrationRuntime` with Anonymous access.
* **ADLS Gen2 Linked Service (`ls_adls_bronze`)**: Account Key authentication pointing to storage container URL (`https://azcriptoprojectstorage.dfs.core.windows.net/`).
* **Pipeline ID**: `pl_ingest_crypto` (Run ID: `bb33df68-3ffd-4e1d-b1ae-b0cd274e26a9`) | **Status**: `Succeeded` | **Duration**: `7m 41s`.

---

### 2. Real-Time Streaming Flow (Azure Logic Apps & Event Hubs Capture)

#### Non-Technical Stream Flow
Instead of collecting data in large batch files, this pipeline continuously captures live Bitcoin market prices in real-time and safely lands them directly into the Azure Data Lake Storage (ADLS Gen2) **Bronze Container**:

1. **Live Data Fetcher (Azure Logic Apps)**: Triggers every 2 minutes to fetch current spot prices from live crypto exchange APIs.
2. **Streaming Event Broker (Azure Event Hubs)**: Receives live price events at high speed without dropping any incoming data packets.
3. **Automated Storage Capture**: Automatically captures and stores incoming live messages directly as raw Avro files into the **Bronze Container** in Azure Data Lake Storage.
4. **Databricks Decoding Engine**: Reads raw binary files from the Bronze container, decodes the encrypted payloads into plain text/JSON, and writes structured records into Unity Catalog Delta Lake tables.

#### Technical Architecture & Azure Metrics
* **Serverless Stream Producer**: Azure Logic App (`live-crypto-producer`) triggers every **2 minutes**, issuing HTTP `GET` requests to CoinGecko REST API (`https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd`) and serializes response payloads directly into Event Hubs.
* **Event Hubs Capture Details**:
  * **Namespace**: `cripto-stream-ns-01` | **Topic**: `cripto-live-trades` | **Partitions**: `2`
  * **Bronze Storage Path**: `abfss://bronze@awcryptoprojectstoracc.dfs.core.windows.net/cripto-stream-ns-01/cripto-live-trades/`
  * **Storage Format**: Encrypted binary Avro (`.avro`).
* **Live Telemetry Metrics**:
  * **Incoming Messages**: ~179 total requests processed across monitoring windows with zero server errors (`Server Errors = 0`).
  * **Landed Volume**: ~87.11 KB of binary Avro files landed directly into the Bronze Container.
  * **Throughput**: Steady ingestion rate peaking between 1.4 KB/s and 2.51 KB/s.

---

## ⚡ Data Processing & Delta Lake Storage Layer (Databricks)

### 📌 Non-Technical Executive Summary
* **What Happens in Databricks?**: Databricks acts as the central processing engine. It takes raw, unorganized crypto data previously stored in ADLS Gen2 Bronze containers and transforms it into structured, clean, and query-ready analytics tables.
* **Business Value**: Raw incoming market files are cleaned of technical index column names, and binary streaming data is decoded so business analysts and dbt models can work with human-readable information.
* **Zero Manual Effort**: Automated Databricks Workflows run on a fixed 4-hour schedule to process new streaming market data automatically.
* **Security & Compliance**: Storage account access keys are never typed directly into script files. Instead, credentials are strictly pulled from secure, encrypted vault scopes.

---

### 🛠️ Technical Implementation Details

#### 1. Key Vault Secret Scope Authentication
* **Security Layer**: Uses Databricks Secret Scopes (`crypto-data-key`) to securely retrieve storage account credentials (`storagekey`) at runtime.
* **ADLS Integration**: Configures Spark sessions on the fly to mount and read directly from Azure Data Lake Storage Gen2 (`awcryptoprojectstoracc`) over secure `abfss://` protocol endpoints.

#### 2. Historical Batch Processing Pipeline (`Batch_processing&transformation`)
* **Raw Data Ingestion**: Reads unheadered CSV historical market trade logs directly from the Azure Data Lake Bronze layer (`/historical_crypto_data/`).
* **Schema Standardization**: Maps abstract raw index columns (`_c0` through `_c6`) to explicit domain field attributes:
  * `_c0` $\rightarrow$ `trade_id`
  * `_c1` $\rightarrow$ `price`
  * `_c2` $\rightarrow$ `qty`
  * `_c3` $\rightarrow$ `quote_qty`
  * `_c4` $\rightarrow$ `time`
  * `_c5` $\rightarrow$ `is_buyer_maker`
  * `_c6` $\rightarrow$ `is_best_match`
* **Delta Persistence**: Appends renamed, schema-enforced datasets into Unity Catalog Delta table `Crypto_project_cat.default.historical_crypto_data`.

#### 3. Live Streaming Extraction Pipeline (`stream data & tranformation`)
* **Avro Streaming Ingestion**: Ingests raw Avro binary log files generated by Azure Event Hubs Capture from `/cripto-stream-ns-01/cripto-live-trades/`.
* **Binary Decoding & Payload Extraction**: Converts the raw binary `Body` field into UTF-8 decoded JSON string strings containing live spot market pricing metrics.
* **Metadata Projection**: Selects stream sequence headers (`SequenceNumber`, `Offset`, `EnqueuedTimeUtc`) along with the decoded payload body `trade_json`.
* **Delta Persistence**: Appends structured stream event records into Unity Catalog Delta table `Crypto_project_cat.default.streaming_crypto_data`.

#### 4. Automated Workflow Scheduling & Monitoring
* **Job Execution**: The streaming transformation pipeline is deployed as an automated Databricks Workflow Job (`stream_data_tranformation`).
* **Trigger Recurrence**: Runs automatically every 4 hours using single-node or multi-node clusters (`Crypto_project 2XS`).
* **Execution Metrics**: Consistently maintains an average run duration between **3m 56s** and **6m 46s** with verified `Succeeded` run statuses across automated cycles.

---

## 🏛️ Unity Catalog Data Dictionary

| Dataset Category | Source Location (ADLS Gen2) | Target Unity Catalog Delta Table | Primary Transformation Applied |
| :--- | :--- | :--- | :--- |
| **Historical Trades** | `bronze/historical_crypto_data/` | `Crypto_project_cat.default.historical_crypto_data` | Renamed raw index columns (`_c0`-`_c6`) into business field names. |
| **Live Price Stream** | `bronze/cripto-stream-ns-01/` | `Crypto_project_cat.default.streaming_crypto_data` | Decoded Avro binary payloads to UTF-8 JSON & extracted stream metadata. |



# dbt Data Transformation Layer: Crypto Analytics Platform

A production-grade **dbt Cloud** data modeling pipeline deployed on **Databricks**, designed to ingest, harmonize, deduplicate, and aggregate high-volume streaming and historical cryptocurrency trade data.

---

## 🏗️ Architecture & Model Lineage

The transformation stack follows Medallion Architecture principles across three distinct dbt layers:

```
[ Raw Lakehouse Catalogs ]
   ├── crypto_project_cat.default.historical_crypto_data
   └── crypto_project_cat.default.streaming_crypto_data
             │
             ▼
[ Staging Layer (staging_crypto) ]
   ├── stg_historical_crypto
   └── stg_streaming_crypto_data
             │
             ▼
[ Intermediate Layer (INTERMEDIATES) ]
   └── int_crypto_trades_combined  (Schema Alignment, Deduplication & Price Rounding)
             │
             ├──────────────────────────────────────────┐
             ▼                                          ▼
[ Marts Layer (CRYPTO_MARTS) ]              [ Testing & DQ Layer ]
   ├── dim_crypto_assets                       ├── source_test.yml (Schema Tests)
   └── fct_crypto_hourly_metrics               └── data_check.sql (Grain Uniqueness)
```

---

## 📁 Repository Structure

```text
End-to-End-crypto-streaming-pipeline/
├── macros/
│   ├── macro_crypto_price.sql       # Custom macro for price rounding & percentage change
├── models/
│   └── crypto/
│       ├── source.yml               # Source declarations & catalog definitions
│       ├── staging_crypto/
│       │   ├── stg_historical_crypto.sql
│       │   └── stg_streaming_crypto_data.sql
│       ├── INTERMEDIATES/
│       │   └── int_crypto_trades_combined.sql
│       ├── CRYPTO_MARTS/
│       │   ├── dim_crypto_assets.sql
│       │   └── fct_crypto_hourly_metrics.sql
│       └── TEST/
│           ├── data_check.sql       # Grain integrity verification query
│           └── source_test.yml      # Model schema tests (unique, not_null)
└── dbt_project.yml
```

---

## 🛠️ Data Pipeline Model Layers

### 1. Source Configuration (`source.yml`)
* **Catalog:** `crypto_project_cat`
* **Schema:** `default`
* **Tables:** Maps `historical_crypto_data` and `streaming_crypto_data` into dbt source references (`{{ source(...) }}`).

### 2. Staging Layer (`staging_crypto`)
* **`stg_historical_crypto.sql`**: 
  * Standardizes datatypes and converts microsecond Unix epochs to timestamp using `TIMESTAMP_MICROS(cast(time AS BIGINT))`.
  * Explicitly casts numeric values (`DOUBLE`) and boolean flags (`is_buyer_maker`, `is_best_match`).
* **`stg_streaming_crypto_data.sql`**:
  * Parses complex nested JSON event payloads from real-time stream buffers (`get_json_object`, `element_at`, `map_keys`).
  * Converts string timestamps to timestamp types (`to_timestamp`).

### 3. Intermediate Layer (`INTERMEDIATES`)
* **`int_crypto_trades_combined.sql`**:
  * **Unified Schema:** Merges streaming and historical batch records via `UNION ALL`, filling source-specific missing attributes with `NULL` and adding provenance metadata (`data_source`).
  * **Deduplication:** Implements window partitioning to prune duplicate ticks:
    ```sql
    ROW_NUMBER() OVER (
        PARTITION BY crypto_name, price_usd, date_trunc('second', event_timestamp) 
        ORDER BY event_timestamp DESC
    ) AS row_num
    ```
  * Filtered where `row_num = 1`. Integrates custom price rounding macro (`{{ round_price(...) }}`).

### 4. Marts Layer (`CRYPTO_MARTS`)
* **`dim_crypto_assets.sql`**: 
  * Enterprise dimension model tracking lifetime asset stats per `crypto_name`.
  * Metrics: `first_trade_timestamp`, `latest_trade_timestamp`, `total_lifetime_ticks`, `all_time_low_price_usd`, `all_time_high_price_usd`, `lifetime_avg_price_usd`, and source tick breakdowns (`historical_lifetime_ticks` vs. `streaming_lifetime_tick`).
* **`fct_crypto_hourly_metrics.sql`**:
  * Aggregated fact table producing hourly financial metrics (OHLC candle generation).
  * Evaluates `open_price_usd` and `close_price_usd` using analytic window functions (`FIRST_VALUE`, `LAST_VALUE`).
  * Derives `hourly_price_spread_usd`, `hourly_price_change_usd`, and price volatility percentages using macros.
  * Captures total trade volume (`total_volume_crypto`, `total_volume_usd`) and market sentiment counts (`buyer_maker_trades_count`, `seller_maker_trades_count`).

---

## 🧪 Data Quality & Testing Strategy

**Primary Key & Schema Validation (`source_test.yml`)**
* `dim_crypto_assets`: `crypto_name` verified with `unique` and `not_null`.
* `fct_crypto_hourly_metrics`: `trade_hour` and `crypto_name` enforced with `not_null` along with core OHLC metric columns (`open_price_usd`, `high_price_usd`, `low_price_usd`, `close_price_usd`).

**Grain Uniqueness Test (`data_check.sql`)**
* Custom query ensuring zero collisions on composite key `(trade_hour, crypto_name)`:
  ```sql
  SELECT 
      trade_hour, 
      crypto_name, 
      COUNT(*) AS row_count 
  FROM {{ ref('fct_crypto_hourly_metrics') }} 
  GROUP BY trade_hour, crypto_name 
  HAVING COUNT(*) > 1
  ```
* **Result:** Returned `0 rows` (Grain integrity validated).

---

## ⚡ Production Validation & Performance

| Model / Table | Engine Target | Verified Record Count | Status |
| :--- | :--- | :--- | :--- |
| `stg_historical_crypto` | Databricks / Delta Lake | **135,946,515** | Verified |
| `int_crypto_trades_combined` | Databricks / Delta Lake | **50,083,348** | Verified (Deduplicated) |
| `fct_crypto_hourly_metrics` | Databricks / Delta Lake | Aggregated Hourly Grain | Verified |

* **Orchestration:** Managed via dbt Cloud Scheduled Job (`Crypto_project_job_orch`).
* **SLA & Reliability:** Maintained a **100.00% Success Rate** across all automated continuous integration and production deployment runs.
*
