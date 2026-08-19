# 🪙 Enterprise Crypto Hybrid Data Engine (Batch & Real-Time)

[![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-0078D4?style=for-the-badge&logo=azure-data-factory&logoColor=white)](https://azure.microsoft.com/en-us/products/data-factory)
[![Azure Logic Apps](https://img.shields.io/badge/Azure%20Logic%20Apps-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/en-us/products/logic-apps)
[![Azure Event Hubs](https://img.shields.io/badge/Azure%20Event%20Hubs-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/en-us/products/event-hubs)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Azure Storage](https://img.shields.io/badge/Azure%20Data%20Lake-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/en-us/products/storage/data-lake-storage/)


================================================================================
          COMPLETE ENTERPRISE CRYPTO PLATFORM: TOOL MAP & MATRIX
================================================================================

--------------------------------------------------------------------------------
1. ARCHITECTURE VISUAL MAP ("PICS" / ICON FLOW)
--------------------------------------------------------------------------------

 [ 🌐 Binance / CoinGecko ] 
       │
       ├── (Batch Backfill)   ---> [ ⚙️ Azure Data Factory ]  ──┐
       │                                                       │
       └── (2-Min Streaming)  ---> [ ⚡ Azure Logic Apps ]     │
                                           │                   │
                                           ▼                   ▼
                                 [ 📡 Azure Event Hubs ] ──> [ 🗄️ ADLS Gen2 (Bronze) ]
                                                                     │
                                                                     ▼
 [ 🔐 Entra ID / Key Vault ] ───────────────────────────────> [ 🧱 Databricks / Spark ]
                                                                     │
                                                                     ▼
 [ 🧪 dbt Cloud Orchestration ] ───────────────────────────> [ 🔺 Delta Lake (Unity Catalog) ]
                                                                     │
                                                                     ▼
                                                             [ 🗄️ ADLS Gen2 (Silver/Gold) ]
                                                                     │
                                                                     ▼
 [ 📊 Plotly / Python ] <─── [ 🔌 PyODBC Token Auth ] <─── [ 🔷 Synapse Serverless SQL ]


 * TOOL ICON DIRECTORY:
   -----------------------------------------------------------------------------
   [ ⚙️ ] Azure Data Factory  : Serverless batch orchestrator pulling historical archives
   [ ⚡ ] Azure Logic Apps     : Serverless API fetcher triggering every 2 minutes
   [ 📡 ] Azure Event Hubs    : Streaming ingestion broker with automatic Avro capture
   [ 🗄️ ] ADLS Gen2           : Object storage hosting Medallion containers (Bronze/Silver/Gold)
   [ 🧱 ] Databricks / Spark  : Distributed processing engine decoding Avro to Delta Lake
   [ 🔺 ] Delta Lake          : ACID-compliant lakehouse storage in Unity Catalog
   [ 🧪 ] dbt Cloud           : Data transformation, hourly OHLC aggregation, & testing
   [ 🔷 ] Azure Synapse       : Zero-copy Serverless SQL engine querying Gold Delta views
   [ 🔐 ] Entra ID / KeyVault : Identity provider, OAuth2 auth, & secret scope manager
   [ 🔌 ] PyODBC Connector   : Python driver executing SQL queries via dynamic access tokens
   [ 📊 ] Plotly & Pandas     : Dark-themed visualization & financial feature engineering


--------------------------------------------------------------------------------
2. END-TO-END DATA PIPELINE WORKFLOW & TOOLS MATRIX
--------------------------------------------------------------------------------

+-----------------------+-----------------------+-----------------------+--------------------------------------------------+
| Workflow Stage        | Tool / Technology     | Tool Category         | System Function & Core Architecture Role         |
+-----------------------+-----------------------+-----------------------+--------------------------------------------------+
| 1. Batch Ingestion    | Azure Data Factory    | Cloud Data Integration| Fetches raw zipped historical trade CSVs from    |
|                       | (ADF Pipelines)       |                       | Binance Vision to ADLS Gen2 Bronze container.     |
+-----------------------+-----------------------+-----------------------+--------------------------------------------------+
| 2. Real-Time Producer | Azure Logic Apps      | Serverless Workflow   | Triggers every 2 mins to pull live BTC spot      |
|                       |                       |                       | pricing payloads from CoinGecko REST API.        |
+-----------------------+-----------------------+-----------------------+--------------------------------------------------+
| 3. Event Streaming    | Azure Event Hubs      | Real-Time Event Broker| Ingests high-frequency streams and auto-captures |
|                       | (Capture Engine)      |                       | payloads directly into Bronze as Avro binary.    |
+-----------------------+-----------------------+-----------------------+--------------------------------------------------+
| 4. Lakehouse Storage  | Azure Data Lake       | Cloud Object Storage  | Stores immutable raw data (Bronze), clean Delta  |
|                       | Storage Gen2 (ADLS)   |                       | tables (Silver), and aggregated marts (Gold).    |
+-----------------------+-----------------------+-----------------------+--------------------------------------------------+
| 5. Stream Processing  | Azure Databricks      | Distributed Compute   | Decodes Avro binary files to UTF-8 JSON, applies  |
|                       | (PySpark / Workflows) | (Apache Spark)        | schema, and schedules 4-hour batch workflows.    |
+-----------------------+-----------------------+-----------------------+--------------------------------------------------+
| 6. Lakehouse Catalog  | Delta Lake            | Storage Format &      | Guarantees ACID transactions, schema enforcement,|
|                       | (Unity Catalog)       | Governance            | and unified catalog management across layers.    |
+-----------------------+-----------------------+-----------------------+--------------------------------------------------+
| 7. Transformation     | dbt Cloud             | Analytics Engineering | Deduplicates 135M+ trades, normalizes schemas,   |
|                       | (Data Modeling)       |                       | and aggregates ticks into hourly OHLC metrics.   |
+-----------------------+-----------------------+-----------------------+--------------------------------------------------+
| 8. Analytics Serving  | Azure Synapse         | Serverless SQL Pool   | Exposes zero-copy Gold Delta views using         |
|                       | Serverless SQL        |                       | OPENROWSET for sub-second analyst querying.      |
+-----------------------+-----------------------+-----------------------+--------------------------------------------------+
| 9. Security & Secrets | Azure Entra ID /      | Identity & Key        | Manages passwordless Managed Identity access,    |
|                       | Databricks Key Vault  | Management            | OAuth tokens, and secure secret scope keys.      |
+-----------------------+-----------------------+-----------------------+--------------------------------------------------+
| 10. Quant Analysis    | Python (Pandas/NumPy) | Data Analytics        | Engineers Order Flow Imbalance, VWAP, Price      |
|                       | & PyODBC              |                       | Drift, and connects securely via token injection.|
+-----------------------+-----------------------+-----------------------+--------------------------------------------------+
| 11. Visual Dashboards | Plotly Express        | Interactive Plotting  | Renders dark-themed time-series area charts,     |
|                       | (Plotly Dark Theme)   |                       | scatter matrices, and SLA threshold lines.       |
+-----------------------+-----------------------+-----------------------+--------------------------------------------------+

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
 
 # Video :
 

https://github.com/user-attachments/assets/ecd6a2dc-5dd3-480e-bb0d-2f7593c08ed4



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
* **Job Execution**: The streaming transformation pipeline is deployed as an automated Databricks Workflow Job (`stream_data_tranformation` & data `landing into containers`).
* **Trigger Recurrence**: Runs automatically every 4 hours using single-node or multi-node clusters (`Crypto_project 2XS`).
* **Execution Metrics**: Consistently maintains an average run duration between **3m 56s** and **6m 46s** with verified `Succeeded` run statuses across automated cycles.

---

## 🏛️ Unity Catalog Data Dictionary

| Dataset Category | Source Location (ADLS Gen2) | Target Unity Catalog Delta Table | Primary Transformation Applied |
| :--- | :--- | :--- | :--- |
| **Historical Trades** | `bronze/historical_crypto_data/` | `Crypto_project_cat.default.historical_crypto_data` | Renamed raw index columns (`_c0`-`_c6`) into business field names. |
| **Live Price Stream** | `bronze/cripto-stream-ns-01/` | `Crypto_project_cat.default.streaming_crypto_data` | Decoded Avro binary payloads to UTF-8 JSON & extracted stream metadata. |

# video



https://github.com/user-attachments/assets/fa107026-fce9-4fc4-a20d-50431dda2f3b





# dbt Enterprise Data Transformation Layer: Crypto Analytics Platform

An enterprise-grade **dbt Cloud** modeling pipeline deployed on **Databricks Delta Lake**, engineered to ingest, harmonize, deduplicate, and aggregate massive streaming and historical cryptocurrency trade volumes (135M+ rows). 

---

## 🎯 Architectural Rationale & Design Philosophy

Directly querying raw streaming tick data (135M+ rows) in downstream BI dashboards (e.g., Power BI) causes severe performance bottlenecks, long query latency, and high engine compute costs. To solve this, this dbt transformation pipeline implements a **Dual-Grain Medallion Architecture**:

1. **Sub-Second / Second-Level Granularity (Intermediate Layer):** Preserves deduplicated, normalized trade-level event logs in `int_crypto_trades_combined`. This serves ad-hoc investigation, fraud detection, and deep-dive historical research directly on Databricks.
2. **Aggregated Hourly Grain (Marts Layer):** Rolls up raw trade ticks into optimized **Hourly OHLC Financial Metrics** (`fct_crypto_hourly_metrics`). This reduces scan volumes by over **99%**, guaranteeing sub-second dashboard rendering times and minimal engine utilization.

---

## 🏗️ End-to-End Pipeline Lineage

```text
[ Raw Lakehouse Catalogs ]
   ├── crypto_project_cat.default.historical_crypto_data (135M+ Ticks)
   └── crypto_project_cat.default.streaming_crypto_data  (Live Event Buffer)
             │
             ▼
[ Staging Layer (staging_crypto) ] ─── Data Type Casting & JSON Unpacking
   ├── stg_historical_crypto
   └── stg_streaming_crypto_data
             │
             ▼
[ Intermediate Layer (INTERMEDIATES) ] ─── Schema Alignment & Tick Deduplication
   └── int_crypto_trades_combined  (Grain: Second-level Trade Event)
             │
             ├──────────────────────────────────────────────────────┐
             ▼                                                      ▼
[ Marts Layer (CRYPTO_MARTS) ] ── Dashboard Optimization      [ Quality & Testing ]
   ├── dim_crypto_assets       (Lifetime Analytics)          ├── source_test.yml
   └── fct_crypto_hourly_metrics (Fast BI Aggregations)      └── data_check.sql
```

---

## 📁 Repository Structure

```text
End-to-End-crypto-streaming-pipeline/
├── macros/
│   └── macro_crypto_price.sql       # Business logic macros (Price rounding & volatility %)
├── models/
│   └── crypto/
│       ├── source.yml               # Source definitions & Delta Lake mapping
│       ├── staging_crypto/
│       │   ├── stg_historical_crypto.sql
│       │   └── stg_streaming_crypto_data.sql
│       ├── INTERMEDIATES/
│       │   └── int_crypto_trades_combined.sql # High-frequency detail layer
│       ├── CRYPTO_MARTS/
│       │   ├── dim_crypto_assets.sql          # Dimension table
│       │   └── fct_crypto_hourly_metrics.sql  # High-performance BI aggregation mart
│       └── TEST/
│           ├── data_check.sql       # Custom grain integrity assertions
│           └── source_test.yml      # Schema contracts (unique, not_null)
└── dbt_project.yml
```

---

## 🛠️ Data Pipeline Model Specifications

### 1. Staging Layer (`staging_crypto`)
* **`stg_historical_crypto.sql`**: Standardizes datatypes, casts microsecond epoch timestamps (`TIMESTAMP_MICROS`), and normalizes Boolean indicators (`is_buyer_maker`, `is_best_match`).
* **`stg_streaming_crypto_data.sql`**: Flattens nested JSON payloads from streaming sources (`get_json_object`, `element_at`, `map_keys`) into typed tabular schema.

### 2. Intermediate Layer (`INTERMEDIATES`)
* **`int_crypto_trades_combined.sql`** *(Preserved Granularity)*:
  * **Schema Harmonization:** Unifies historical batch backfills and real-time streams via `UNION ALL` while tracking provenance (`data_source`).
  * **Windowed Deduplication:** Removes duplicate tick hits caused by stream retries using window partitioning:
    ```sql
    ROW_NUMBER() OVER (
        PARTITION BY crypto_name, price_usd, date_trunc('second', event_timestamp) 
        ORDER BY event_timestamp DESC
    ) AS row_num
    ```
  * **Purpose:** Acts as the Single Source of Truth (SSOT) for micro-level trade analysis without compromising dashboard load times.

### 3. Marts Layer (`CRYPTO_MARTS`)
* **`fct_crypto_hourly_metrics.sql`** *(BI Acceleration Mart)*:
  * Aggregates millions of second-level records into structured **Hourly OHLC Candlesticks**.
  * Derives `open_price_usd` and `close_price_usd` using window functions (`FIRST_VALUE`, `LAST_VALUE`).
  * Calculates price spreads, percentage volatility, volume metrics (`total_volume_crypto`, `total_volume_usd`), and market sentiment indicators (`buyer_maker_trades_count` vs `seller_maker_trades_count`).
* **`dim_crypto_assets.sql`**: Tracks cumulative asset metadata, lifetime price ranges (All-Time High/Low), and streaming vs historical tick ratios per crypto pair.

---

## 🧪 Data Governance & Quality Enforcement

* **Primary Key & Nullability Contracts (`source_test.yml`)**: Enforces strict `unique` and `not_null` constraints on primary dimension keys and critical metrics (`trade_hour`, `open_price_usd`, `close_price_usd`).
* **Grain Integrity Assertion (`data_check.sql`)**: Custom automated test preventing fan-out errors or duplicate hour-pair combinations:
  ```sql
  SELECT 
      trade_hour, 
      crypto_name, 
      COUNT(*) AS row_count 
  FROM {{ ref('fct_crypto_hourly_metrics') }} 
  GROUP BY trade_hour, crypto_name 
  HAVING COUNT(*) > 1
  ```
  * **Result:** `0 rows returned` (Grain uniqueness 100% verified).

---

## ⚡ Scale & Performance Metrics

| Model / Table Layer | Granularity | Record Volume | Primary Consumer / Use Case |
| :--- | :--- | :--- | :--- |
| `stg_historical_crypto` | Raw Event Tick | **135,946,515** | Staging Backfill Buffer |
| `int_crypto_trades_combined` | Second-level Detail | **50,083,348** | Ad-hoc Deep Dives & Telemetry |
| `fct_crypto_hourly_metrics` | Hourly Rollup | **Aggregated** | **Power BI & Fast Reporting** |

* **Orchestration:** Automated via dbt Cloud Production Orchestration (`Crypto_project_job_orch`).
* **Operational SLA:** **100% Success Rate** across scheduled automated execution runs on Databricks compute.



*

https://github.com/user-attachments/assets/55e7b127-0ee6-44b7-9ddb-ca574852441b




================================================================================
          EXECUTIVE CRYPTO ANALYTICS & DATA PIPELINE ARCHITECTURE
================================================================================

--------------------------------------------------------------------------------
1. PYTHON CODE (VISUALS 4 & 5 FROM JUPYTER NOTEBOOK)
--------------------------------------------------------------------------------

import plotly.express as px

# --- Visual 4: Ingestion Pipeline Health vs SLA ---
fig_pipeline_ingestion_health = px.line(
    df,
    x="trade_hour",
    y="streaming_pipeline_health",
    color="crypto_name",
    title="<b>4. Pipeline Ingestion Health (Streaming vs Batch Contribution)</b>",
    labels={
        "streaming_pipeline_health": "Streaming Ratio (0.0 to 1.0)",
        "trade_hour": "Trade Hour"
    },
    template="plotly_dark"
)
fig_pipeline_ingestion_health.add_hline(
    y=0.8, line_dash="dot", line_color="yellow", annotation_text="80% SLA Threshold"
)
fig_pipeline_ingestion_health.show()


# --- Visual 5: Structural Lifetime Price Drift (%) ---
fig_str_p_d = px.area(
    df,
    x="trade_hour",
    y="lifetime_price_drift_pct",
    color="crypto_name",
    title="<b>5. Structural Lifetime Price Drift (%)</b>",
    labels={
        "lifetime_price_drift_pct": "Drift Deviation (%)",
        "trade_hour": "Trade Hour"
    },
    template="plotly_dark"
)
fig_str_p_d.show()


--------------------------------------------------------------------------------
2. ROOT-CAUSE ANALYTICAL INSIGHTS & BUSINESS RATIONALE ("THE WHY")
--------------------------------------------------------------------------------

[ METRIC 1: PIPELINE INGESTION HEALTH & SLA TRANSITION ]

  * OBSERVATION:
    The streaming ingestion ratio linearly increases from 0.0 (Jan 2025) up to 
    ~0.90 (Jul 2026), officially crossing the 80% SLA threshold in May 2026.

  * WHY THIS METRIC MATTERS & WHAT CAUSES IT:
    - Legacy Reliance: Early 2025 relied almost entirely on batch scheduled 
      ELT pipelines, causing high latency (1 to 24 hours behind real-time).
    - Architecture Migration: The steady upward trend represents an active 
      engineering migration toward real-time message streaming (e.g., Kafka / 
      Event Hubs / Spark Streaming).
    - Business Impact: Low streaming health breaches institutional SLAs. Reaching 
      >80% streaming coverage ensures that downstream automated trading models 
      and Synapse analytical dashboards query near real-time data rather than 
      stale batch snapshots.


[ METRIC 2: STRUCTURAL LIFETIME PRICE DRIFT (%) ]

  * OBSERVATION:
    Initial high noise (+10% to -10%) in early 2025 gives way to a sustained, 
    expanding negative drift downward to -35% / -40% by mid-2026.

  * WHY THIS METRIC MATTERS & WHAT CAUSES IT:
    - Regime Shift Detection: Price drift tracks structural decoupling of current 
      spot prices from historical lifetime moving averages. 
    - Macro Valuation Compression: The steady negative drift indicates a long-term 
      bearish structural trend relative to the asset's historical baseline.
    - Risk & Quant Strategy Impact: Algorithms relying on standard mean-reversion 
      fail during structural drift. Tracking this percentage forces trading models 
      to dynamically decay historical weights and recalibrate risk parameters 
      rather than assuming prices will return to legacy averages.


[ METRIC 3: ORDER FLOW IMBALANCE RATIO ]

  * OBSERVATION:
    Oscillations remain tightly bounded around the zero-line (neutral boundary).

  * WHY THIS METRIC MATTERS & WHAT CAUSES IT:
    - Microstructure Aggression: Measures buy-side maker volume versus sell-side 
      maker volume normalized by total ticks.
    - Market Neutrality: Oscillations near y=0 mean neither buyers nor sellers 
      permanently dominate trade execution. Sudden sustained spikes above +0.5 
      or below -0.5 serve as early warning indicators of institutional liquidity 
      sweeps prior to major price breakouts.


[ METRIC 4: WHALE DETECTION & LIQUIDITY MATRIX ]

  * OBSERVATION:
    Retail transactions form a dense baseline cluster (<$100M total volume, low 
    trade sizes), while rare institutional trades stand out as distant outliers.

  * WHY THIS METRIC MATTERS & WHAT CAUSES IT:
    - Market Impact Mitigation: Large trades executed as single orders cause 
      massive slippage and order-book depletion.
    - Execution Rationale: Isolating average trade size against volume allows 
      detecting whether high volume is driven by organic retail participation 
      or institutional algorithmic execution (TWAP/VWAP iceberg orders).




      video:

https://github.com/user-attachments/assets/510b8452-0a86-4a18-80d0-67747a35c458




# Data Landing & Azure Synapse Analytics Serving Layer

This repository documents the end-to-end data landing mechanism from **Azure Databricks** to **Azure Data Lake Storage Gen2 (ADLS Gen2)**, along with the zero-copy serving layer configured in **Azure Synapse Analytics**.

---

## 1. System Architecture & Component Flow

The pipeline moves processed datasets from Databricks catalog tables into physical Delta Lake format across storage tiers, making them instantly queryable in Synapse Serverless SQL without data duplication:

1. **Databricks Notebook (`Data_landing into containers gold & silver`)**: Extracts catalog tables and persists them as Delta Lake tables in ADLS Gen2.
2. **ADLS Gen2 Storage Account (`awcriptoprojectstoracc`)**: 
   * **Silver Container**: Stores raw historical and streaming datasets.
   * **Gold Container**: Stores business-ready dimension and hourly metric fact tables.
3. **Azure Synapse Analytics (`GoldAnalytics` Database)**: Configures passwordless authentication and Serverless SQL views (`OPENROWSET`) directly over Delta files.
4. **Synapse Integration Pipeline (`triger_crypto`)**: Automates execution and synchronization.

---

## 2. Data Landing Layer (Databricks PySpark Logic)

The PySpark notebook handles secure storage authentication and writes catalog data into specific ADLS Gen2 storage containers:

* **Security & Authentication**:
  * Eliminates hardcoded credentials by retrieving storage keys dynamically using Databricks Secret Scope (`crypto-data-key` / `storagekey`).
  * Configures Spark session options (`fs.azure.account.key`) to authorize direct writes to ADLS Gen2 over the ABFSS protocol.

* **Gold Container Operations (`gold`)**:
  * **`fct_crypto_hourly_metrics`**: Landed as a Delta table from catalog table `crypto_project_cat.default_default.fct_crypto_hourly_metrics` for high-speed hourly analytical queries.
  * **`dim_crypto_assets`**: Landed as a Delta table from `crypto_project_cat.default_default.dim_crypto_assets` to serve as the master dimension reference.

* **Silver Container Operations (`silver`)**:
  * **`historical_crypto_data`**: Landed as a Delta table from `crypto_project_cat.default.historical_crypto_data` to preserve granular batch history.
  * **`streaming_crypto_data`**: Landed as a Delta table from `crypto_project_cat.default.streaming_crypto_data` to store ingested real-time telemetry.

---

## 3. Azure Synapse Serving Layer (Serverless SQL Logic)

Azure Synapse Analytics acts as the virtual serving layer over the Gold storage container using two core SQL scripts:

### A. Credentials & External Infrastructure Setup (`credentials_synapse`)
* **Database Master Key**: Created in the `GoldAnalytics` database to secure database-level credentials.
* **Database Scoped Credential (`SynapseStorageCredential`)**: Configured using **Azure Managed Identity** (`WITH IDENTITY = 'Managed Identity'`) for secure, passwordless authentication between Synapse and ADLS Gen2.
* **External Data Source (`GoldStorageSource`)**: Defines an external endpoint targeting `https://awcryptoprojdataricks.dfs.core.windows.net/gold` using the Managed Identity credential.
* **External File Format (`ParquetFormat`)**: Registers standard Parquet formatting definitions for external interactions.

### B. Delta Lake Serverless Views (`data_view`)
* **`dim_crypto_assets` View**: Reads Delta Lake files directly from the Gold container (`BULK 'dim_crypto_assets'`) using `OPENROWSET` over `GoldStorageSource`.
* **`fct_crypto_hourly_metrics` View**: Maps the hourly metric Delta files (`BULK 'fct_crypto_hourly_metrics'`) using `OPENROWSET`, allowing downstream dashboards and analyst SQL queries to query live data with sub-second response times.

---

## 4. Pipeline Orchestration & Trigger Mechanics

Data synchronization and workflow orchestration are managed inside Azure Synapse Studio:

* **Pipeline Name**: `triger_crypto`
* **Orchestration Activity**: Contains wait and execution activities (`triger`) that coordinate storage refresh and view availability.
* **Trigger Schedule**:
  * **Recurrent Schedule Trigger**: Configured on an **8-Hour UTC recurrence interval** to periodically sync landing layers and update analytical views.
  * **Manual Execution**: Supports ad-hoc, manual triggering (`Trigger Now`) for instant testing and backfills.
* **Monitoring & SLA**: Verified run status of **Succeeded** in Synapse Activity Monitoring, ensuring pipeline reliability and zero downtime.
*

# video: 



https://github.com/user-attachments/assets/696b542b-3292-4571-a056-5f1b0c32fed9



--------------------------------------------------------------------------------
# Python Insights & Feature Engineering
--------------------------------------------------------------------------------

* AZURE SECURE AUTHENTICATION & DATA INGESTION:
  - Token Fetching: Acquires short-lived OAuth access tokens from Azure Entra 
    ID (Active Directory) using Interactive Browser Credentials.
  - Endpoint Target: Injects the token directly into the pyodbc driver connection 
    attribute (SQL_COPT_SS_ACCESS_TOKEN = 1256) to query Gold-layer Fact and 
    Dimension tables directly from Azure Synapse Serverless SQL Pool 
    (awcryptoproject-ondemand.sql.azuresynapse.net / GoldAnalytics DB).

* FEATURE ENGINEERING & VECTORIZED TRANSFORMATIONS:
  - Order Flow Imbalance Ratio: Evaluates buyer vs. seller maker aggression 
    scale (-1.0 to +1.0) by normalizing net maker trade differences over total ticks.
  - Trade Sizing & Volume Weighting: Computes Average Trade Size (USD Volume / 
    Tick Count) and Volume-Weighted Average Price (VWAP = Total USD Volume / 
    Total Crypto Volume).
  - Price Drift & Volatility Scaling: Measures Structural Price Drift (%) by 
    evaluating current hourly prices against lifetime moving averages, and scales 
    hourly price spreads against historical baseline to derive Normalized Volatility (%).
  - Pipeline Telemetry: Monitors operational health (0.0 to 1.0 ratio) by tracking 
    real-time streaming tick contributions against total tick ingestion.

* INTERACTIVE VISUALIZATION ENGINE:
  - Dashboard Rendering: Leverages Plotly Express with dark styling ('plotly_dark') 
    to generate interactive time-series area fills, multi-variable scatter matrices, 
    and horizontal SLA threshold reference boundaries.


--------------------------------------------------------------------------------
2. REFINED ROOT-CAUSE ANALYTICAL INSIGHTS & BUSINESS RATIONALE ("THE WHY")
--------------------------------------------------------------------------------

[ INGESTION PIPELINE HEALTH VS 80% SLA THRESHOLD ]

  * OBSERVATION:
    The streaming health ratio steadily rises from 0.0 (Jan 2025 batch-only) 
    to cross the 80% SLA threshold in May 2026, reaching ~90% real-time streaming 
    coverage by mid-2026.

  * BUSINESS RATIONALE ("THE WHY"):
    - Legacy Bottleneck: Early 2025 relied on batch-scheduled ELT pipelines, 
      causing analytical dashboards to run on 1-to-24-hour delayed snapshots.
    - Architecture Migration: The upward trajectory represents an active engineering 
      transition to real-time event streaming (Kafka / Azure Event Hubs).
    - Operational Impact: Crossing the 80% SLA threshold guarantees that 
      downstream quantitative models and Synapse reporting layers consume live 
      order-book telemetry rather than stale batch data.


[ STRUCTURAL LIFETIME PRICE DRIFT (%) ]

  * OBSERVATION:
    High initial noise (+10% to -10%) in Q1 2025 transitions into a sustained 
    downward drift from mid-2025 through 2026, settling at -35% to -40% deviation 
    relative to the lifetime historical average.

  * BUSINESS RATIONALE ("THE WHY"):
    - Regime Shift Detection: Standard mean-reversion algorithms assume asset 
      prices naturally decay back toward historical baseline averages.
    - Risk & Model Calibration: The persistent negative drift proves a structural 
      macro valuation shift. Tracking this metric forces automated trading 
      algorithms to decay historical weights and adjust risk parameters rather 
      than making faulty mean-reversion assumptions.


[ ORDER FLOW IMBALANCE RATIO ]

  * OBSERVATION:
    Cross-asset order imbalance metrics tightly oscillate around the neutral 
    zero boundary line (y=0).

  * BUSINESS RATIONALE ("THE WHY"):
    - Microstructure Aggression: Measures buy-side maker volume versus sell-side 
      maker volume normalized across trade ticks.
    - Market State: Tightly bounded oscillation confirms market neutrality 
      without continuous directional bias. Sustained breakouts beyond neutral 
      thresholds (+0.5 or -0.5) serve as early warning indicators for institutional 
      liquidity sweeps prior to major price breakouts.


[ WHALE ACTIVITY & LIQUIDITY MATRIX ]

  * OBSERVATION:
    Retail transactions dense-cluster below $100M total volume with low average 
    trade sizes ($500–$1,500), whereas institutional whale transactions isolate 
    as distinct high-volume outliers ($300M+ total volume).

  * BUSINESS RATIONALE ("THE WHY"):
    - Execution Impact: Institutional traders avoid single market-sweep orders 
      to prevent order-book depletion and severe price slippage.
    - Algorithmic Tracking: Plotting average trade size against total volume 
      helps distinguish organic retail participation from algorithmic TWAP/VWAP 
      iceberg execution utilized by institutional whales.



<img width="1094" height="385" alt="newplot (17)" src="https://github.com/user-attachments/assets/22a66fe6-eaab-45ff-a5aa-7d1bd93edf48" />
      
<img width="1094" height="385" alt="newplot (16)" src="https://github.com/user-attachments/assets/c7ce40eb-06e5-4206-9df1-43ce55bd9428" />

<img width="1094" height="385" alt="newplot (15)" src="https://github.com/user-attachments/assets/c7f0d439-d5e0-4ae5-b237-a8f8250f7606" />

<img width="1094" height="373" alt="newplot (14)" src="https://github.com/user-attachments/assets/354eeaa8-bd39-49b3-aa6c-9af6c155adc2" />

<img width="1094" height="385" alt="newplot (13)" src="https://github.com/user-attachments/assets/72784857-ee0c-49d2-a3d5-62910122844f" />





