# Data Flow

## Overview

The Netflix Data Engineering Pipeline processes Netflix catalog data through a modern Databricks Medallion Architecture.

The source data is provided as CSV files and prepared into multiple batches to simulate incremental data arrival.

The data flows through:

**Source CSV Batches → Databricks Volume → Auto Loader → Bronze → Silver → Gold → Databricks SQL Dashboard**

Lakeflow Jobs orchestrates the Bronze, Silver, and Gold pipelines in dependency order.

---

## 1. Source Data

The project uses a public Netflix catalog dataset containing information about Netflix movies and TV shows.

The source data consists of five CSV files:

- `netflix_titles.csv`
- `netflix_cast.csv`
- `netflix_category.csv`
- `netflix_countries.csv`
- `netflix_directors.csv`

The source data was divided into multiple batches to simulate incremental file arrival.

Example: 
```text
batch_001/
├── netflix_titles.csv
├── netflix_cast.csv
├── netflix_category.csv
├── netflix_countries.csv
└── netflix_directors.csv

batch_002/
├── netflix_titles.csv
├── netflix_cast.csv
├── netflix_category.csv
├── netflix_countries.csv
└── netflix_directors.csv

batch_003/
├── netflix_titles.csv
├── netflix_cast.csv
├── netflix_category.csv
├── netflix_countries.csv
└── netflix_directors.csv 
```

## 2. Bronze Layer

The Bronze layer is responsible for the initial ingestion of the source files.

Databricks Auto Loader is used with the cloudFiles format to detect and process newly arriving files.

The Bronze pipeline creates five streaming tables:
```
netflix_catalog.bronze.titles
netflix_catalog.bronze.cast
netflix_catalog.bronze.category
netflix_catalog.bronze.countries
netflix_catalog.bronze.directors
```
The Bronze layer keeps the source data close to its original form and performs only the transformations required for ingestion.

Incremental Ingestion

When a new batch is uploaded to the landing Volume, Auto Loader detects the newly available files.
```
For example:

batch_001
    ↓
Auto Loader
    ↓
Bronze tables

batch_002
    ↓
Auto Loader
    ↓
Only newly available files are processed
    ↓
Bronze tables updated
```
This allows the pipeline to process data incrementally instead of repeatedly processing the complete source dataset.

---

## 3. Silver Layer

The Silver layer performs cleaning, standardization, and data-quality processing on the Bronze tables.

The Silver pipeline produces five tables:
```
netflix_catalog.silver.titles
netflix_catalog.silver.cast
netflix_catalog.silver.category
netflix_catalog.silver.countries
netflix_catalog.silver.directors
```
The transformations include:
```
Data type conversion
Whitespace handling
Null and empty-value handling
Duplicate handling
Standardization of values
Relationship validation
Creation of derived fields where required
```
Data-quality checks were performed during the Silver transformation stage.

---

## 4. Gold Layer

The Gold layer contains business-ready datasets created from the Silver tables.

The project contains five Gold datasets:

```
netflix_catalog.gold.content_overview
netflix_catalog.gold.content_by_country
netflix_catalog.gold.content_by_category
netflix_catalog.gold.top_cast_members
netflix_catalog.gold.top_directors
```
The Gold layer applies the required joins and aggregations to make the data suitable for analytical consumption.

The datasets support questions such as:

What is the overall Netflix content distribution?
How is content distributed by country?
Which categories contain the most titles?
Which cast members appear across the most titles?
Which directors are associated with the most titles?

---

## 5. Orchestration

The three Lakeflow Declarative Pipelines are orchestrated using Lakeflow Jobs.
```
The workflow is:

Bronze Ingestion
       ↓
Silver Transformations
       ↓
Gold Analytics
```
The dependency ensures that:

Bronze completes before Silver starts.
Silver completes before Gold starts.
Gold is refreshed using the latest available Silver data.

This provides a single end-to-end workflow for the data pipeline.

---

## 6. Data Consumption

The Gold layer is consumed directly inside Databricks using a Databricks SQL Dashboard.

The dashboard provides a business-facing view of the processed data.

The dashboard includes:

Netflix content overview
Movies vs TV Shows
Content added over time
Category analysis
Other Gold-layer analytical views


---

## 7. Complete Data Flow

The complete pipeline can be summarized as:
```
Public Netflix Catalog Dataset
            │
            ▼
      CSV Source Batches
            │
            ▼
    Databricks Volume
            │
            ▼
       Auto Loader
            │
            ▼
    ┌─────────────────┐
    │  Bronze Layer   │
    │  5 Tables       │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │  Silver Layer   │
    │  5 Tables       │
    │  + Data Quality │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │   Gold Layer    │
    │  5 Datasets     │
    └────────┬────────┘
             │
             ▼
     Databricks SQL
        Dashboard
```
Lakeflow Jobs orchestrates the processing sequence:

Bronze → Silver → Gold

The pipeline was tested by uploading the source batches incrementally and confirming that the data propagated successfully through the Bronze, Silver, and Gold layers.
