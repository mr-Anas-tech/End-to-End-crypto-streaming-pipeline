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

