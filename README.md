# End-to-End-crypto-streaming-pipeline
# 🪙 Crypto Market Data Ingestion Pipeline (Batch Layer)

[![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-0078D4?style=for-the-badge&logo=azure-data-factory&logoColor=white)](https://azure.microsoft.com/en-us/products/data-factory)
[![Azure Data Lake Gen2](https://img.shields.io/badge/Azure%20Data%20Lake%20Storage-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com/en-us/products/storage/data-lake-storage/)
[![Binance Public Data](https://img.shields.io/badge/Source-Binance%20Vision-F3BA2F?style=for-the-badge&logo=binance&logoColor=black)](https://data.binance.vision/)
[![Pipeline Status](https://img.shields.io/badge/Pipeline%20Status-Succeeded-brightgreen?style=for-the-badge)](https://azure.microsoft.com)

---

## 📌 Executive Summary (For Non-Technical Readers)

### 💡 What Does This Project Do?
This project automates the retrieval of raw, historical cryptocurrency trading data (specifically **BTC/USDT** spot market trades) directly from public exchanges like **Binance Vision** and securely moves it into a centralized cloud storage environment on **Microsoft Azure**.

### 🎯 Business Value & Objectives
* **Automated Data Landing**: Eliminates manual downloads by scheduling automated pipeline runs to fetch compressed historical monthly archives.
* **Cost Efficiency**: Pulls compressed files (`.zip`) directly across web endpoints to reduce data transfer overhead before storing them in Azure Data Lake Storage (Bronze Layer).
* **Foundation for Analytics**: Serves as the reliable **Batch Ingestion Foundation** for downstream data transformation, AI/ML price forecasting models, and executive dashboards.

---

## 🏗️ Architecture & Data Flow

Below is the end-to-end lineage of how historical data flows from the source web endpoint into the Azure Cloud storage destination:

┌────────────────────────────────────────────────────────┐
│               Public Source Endpoint                   │
│        Binance Vision Data Repository (HTTP)           │
│   (URL: [https://data.binance.vision/data/spot/](https://data.binance.vision/data/spot/)...)     │
└──────────────────────────┬─────────────────────────────┘
                           │
                           │ HTTP GET Method
                           ▼
┌────────────────────────────────────────────────────────┐
│               Azure Data Factory (ADF)                 │
│             Pipeline: pl_ingest_crypto                 │
│                                                        │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Activity: Copy data1 (Copy Activity)             │  │
│  │  • Linked Service Source : Httpcryptoproject     │  │
│  │  • Linked Service Sink   : ls_adls_bronze        │  │
│  └──────────────────────────────────────────────────┘  │
└──────────────────────────┬─────────────────────────────┘
                           │
                           │ Secure Cloud Transfer (ZipDeflate Stream)
                           ▼
┌────────────────────────────────────────────────────────┐
│             Azure Data Lake Storage Gen2               │
│           Account: azcriptoprojectstorage              │
│                                                        │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Path: bronze / historical_crypto_data            │  │
│  │ File: BTCUSDT-trades-2025-01.zip                 │  │
│  └──────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────┘
