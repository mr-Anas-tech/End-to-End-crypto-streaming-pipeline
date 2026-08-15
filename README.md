# 🪙 Enterprise Crypto Hybrid Data Engine (Batch & Streaming) Data Source & Data Landing


## 📌 Business Overview & Hybrid Strategy

This project delivers an end-to-end data engineering platform on **Microsoft Azure** designed to capture both historical trade archives and high-frequency live market signals for cryptocurrency pairs (focusing on BTC/USDT).

### 🎯 Key Engineering Goals
* **Dual-Ingestion Pipeline**: Integrates high-volume **Batch historical backfills** with low-latency **Real-Time price streaming** within a unified cloud architecture.
* **Cost-Optimized Ingestion**: Utilizes serverless orchestration (Azure Logic Apps & Data Factory) to eliminate continuous compute costs while fetching compressed raw files and REST API payloads.
* **Medallion Architecture Foundation**: Raw historical zips and live JSON streams land directly into the **Bronze Storage Layer**, forming an immutable landing zone ready for PySpark/Databricks transformation.

---

## 🏗️ Technical Architecture & Data Ingestion Flow

### 1. Batch Historical Flow (Azure Data Factory)
1. **Public Source**: Connects to the public **Binance Vision** HTTP file repository (`https://data.binance.vision/`).
2. **Orchestration**: The ADF pipeline `pl_ingest_crypto` executes an HTTP `GET` request using the `month_param` dataset.
3. **Data Landing**: Streams the raw compressed `.zip` payload directly into **Azure Data Lake Storage Gen2** (`azcriptoprojectstorage`) under the `bronze/historical_crypto_data` directory using a **Flatten Hierarchy** copy strategy.

### 2. Real-Time Streaming Flow (Serverless Event Producer)
1. **Polling Trigger**: An Azure Logic App (`live-crypto-producer`) triggers every **2 minutes** on a serverless cron schedule.
2. **API Data Extraction**: Executes an HTTP `GET` request against the **CoinGecko REST API** (`https://api.coingecko.com/api/v3/simple/price`) to capture current spot metrics for BTC/USD.
3. **Event Hub Dispatch**: Serializes the REST HTTP response payload as a string and publishes it directly into an **Azure Event Hubs** topic (`cripto-live-trades`).

---

## ⚙️ Azure Data Factory (Batch Module Configurations)

### Connections & Linked Services
* **`Httpcryptoproject`**: HTTP Linked Service targeted at Binance Vision using `AutoResolveIntegrationRuntime` with Anonymous access.
* **`ls_adls_bronze`**: ADLS Gen2 Linked Service configured with Account Key authentication pointing to the target storage container URL (`https://azcriptoprojectstorage.dfs.core.windows.net/`).

### Ingestion Dataset Specifications
* **Source (`month_param`)**: Binary HTTP Dataset configured to extract `data/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2025-01.zip` using `ZipDeflate (.zip)` optimal compression.
* **Sink (`ds_adls_binary_sink`)**: Binary ADLS Gen2 Dataset pointing to container `bronze` and path `historical_crypto_data`.

### Run Verification & Performance
* **Pipeline Name**: `pl_ingest_crypto`
* **Run ID**: `bb33df68-3ffd-4e1d-b1ae-b0cd274e26a9`
* **Status**: `Succeeded`
* **Duration**: `7m 41s`

---

## ⚡ Azure Logic Apps (Real-Time Streaming Module)

### Workflow Actions (`live-crypto-producer`)
* **Trigger (`Recurrence`)**: Runs continuously at **2-minute** intervals.
* **Fetch Price (`HTTP`)**: Queries CoinGecko API (`ids=bitcoin&vs_currencies=usd`) returning real-time spot price JSON.
* **Publish Event (`Send event`)**: Uses connection `new_conn_3522a` to push `string(body('HTTP'))` payload into Event Hub `cripto-live-trades`.

### Execution Latency & Metrics
* **Status**: Verified `Succeeded` across consecutive automated cycles.
* **Publish Latency**: Sub-second event delivery (**200ms – 270ms** per execution).







# ⚡ Databricks Processing & Delta Lake Storage Layer

## 📌 Non-Technical & Executive Overview

### What Happens in Databricks?
Databricks acts as the central processing engine for this platform. It takes the raw, unorganized crypto data previously stored in Azure Data Lake Storage (ADLS Gen2) and transforms it into structured, clean, and query-ready analytics tables. 

### Why Is This Important for the Business?
* **Turn Raw Data into Business Value**: Raw incoming market files are difficult to analyze directly. Databricks cleans up technical column names and decodes streaming data so that business analysts and dbt models can work with human-readable information.
* **Zero Manual Effort**: Automated Databricks Workflows run on a fixed 4-hour schedule. This ensures that new streaming market data is processed automatically without needing any engineer to intervene manually.
* **Security & Compliance**: Storage account access keys are never typed directly into script files. Instead, credentials are strictly pulled from secure, encrypted vault scopes.

---

## 🛠️ Technical Implementation Details

### 1. Key Vault Secret Scope Authentication
* **Security Layer**: Uses Databricks Secret Scopes (`crypto-data-key`) to securely retrieve storage account credentials (`storagekey`) at runtime.
* **ADLS Integration**: Configures Spark sessions on the fly to mount and read directly from Azure Data Lake Storage Gen2 (`awcryptoprojectstoracc`) over secure `abfss://` protocol endpoints.

### 2. Historical Batch Processing Pipeline (`Batch_processing&transformation`)
* **Raw Data Ingestion**: Reads unheadered CSV historical market trade logs directly from the Azure Data Lake Bronze layer (`/historical_crypto_data/`).
* **Schema Standardization**: Maps abstract raw index columns (`_c0` through `_c6`) to explicit domain field attributes:
  * `_c0` $\rightarrow$ `trade_id`
  * `_c1` $\rightarrow$ `price`
  * `_c2` $\rightarrow$ `qty`
  * `_c3` $\rightarrow$ `quote_qty`
  * `_c4` $\rightarrow$ `time`
  * `_c5` $\rightarrow$ `is_buyer_maker`
  * `_c6` $\rightarrow$ `is_best_match`
* **Delta Persistence**: Appends the renamed, schema-enforced dataset into the Unity Catalog Delta table `Crypto_project_cat.default.historical_crypto_data`.

### 3. Live Streaming Extraction Pipeline (`stream data & tranformation`)
* **Avro Streaming Ingestion**: Ingests raw Avro binary log files generated by Azure Event Hubs Capture from `/cripto-stream-ns-01/cripto-live-trades/`.
* **Binary Decoding & Payload Extraction**: Converts the raw binary `Body` field into UTF-8 decoded JSON string strings containing live spot market pricing metrics.
* **Metadata Projection**: Selects stream sequence headers (`SequenceNumber`, `Offset`, `EnqueuedTimeUtc`) along with the decoded payload body `trade_json`.
* **Delta Persistence**: Appends structured stream event records into the Unity Catalog Delta table `Crypto_project_cat.default.streaming_crypto_data`.

### 4. Automated Workflow Scheduling & Monitoring
* **Job Execution**: The streaming transformation pipeline is deployed as an automated Databricks Workflow Job (`stream_data_tranformation`).
* **Trigger Recurrence**: Runs automatically every 4 hours using single-node or multi-node clusters (`Crypto_project 2XS`).
* **Execution Metrics**: Consistently maintains an average run duration between **3m 56s** and **6m 46s** with verified `Succeeded` run statuses across automated cycles.

---

## 🏛️ Unity Catalog Data Dictionary

| Dataset Category | Source Location (ADLS Gen2) | Target Unity Catalog Delta Table | Primary Transformation Applied |
| :--- | :--- | :--- | :--- |
| **Historical Trades** | `bronze/historical_crypto_data/` | `Crypto_project_cat.default.historical_crypto_data` | Renamed raw index columns (`_c0`-`_c6`) into business field names. |
| **Live Price Stream** | `bronze/cripto-stream-ns-01/` | `Crypto_project_cat.default.streaming_crypto_data` | Decoded Avro binary payloads to UTF-8 JSON & extracted stream metadata. |

