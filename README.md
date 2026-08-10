# NYC Taxi End-to-End Data Engineering Pipeline

## Project Overview

This project implements an end-to-end data engineering pipeline for processing NYC Taxi trip data using Microsoft Azure.

The pipeline follows a Bronze-Silver-Gold architecture. Azure Data Factory is used for data ingestion, Azure Data Lake Storage Gen2 is used for data storage, and Azure Databricks with PySpark is used for data transformation and processing.

## Architecture

NYC Taxi Data
        ↓
Azure Data Factory
        ↓
ADLS Gen2 - Bronze Layer
        ↓
Azure Databricks + PySpark
        ↓
Silver Layer
        ↓
Gold Layer - Delta Tables

## Technologies Used

- Microsoft Azure
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Apache Spark
- Delta Lake
- GitHub

## Pipeline Flow

### 1. Data Ingestion

Azure Data Factory is used to dynamically ingest NYC Taxi data from the HTTP source into Azure Data Lake Storage Gen2.

### 2. Bronze Layer

The raw NYC Taxi Parquet files are stored in the Bronze layer of ADLS Gen2.

The Bronze layer preserves the source data in its original/raw form.

### 3. Silver Layer

Azure Databricks reads the Bronze data from ADLS Gen2 using secure service principal authentication.

PySpark is used to perform data cleansing and transformation, including:

- Data type transformations
- Null handling
- Duplicate removal
- Column transformations
- Data quality processing

The transformed data is written to the Silver layer.

### 4. Gold Layer

The Silver data is further processed in Azure Databricks to create the Gold layer.

The Gold data is stored using Delta tables to provide curated and analytical-ready data.

## Repository Structure

azure-end-to-end-nyc-taxi-data-pipeline/
│
├── adf/
│   ├── ARMTemplateForFactory.json
│   ├── ARMTemplateParametersForFactory.json
│   ├── linkedTemplates/
│   ├── pipelines/
│   ├── datasets/
│   └── linkedServices/
│
├── bronze/
│   ├── sample/
│   │   ├── green_tripdata_2023-01.parquet
│   │   └── README.md
│   └── README.md
│
├── databricks/
│   └── notebooks/
│       ├── NYC_Taxi_Silver_Transformation.py
│       └── NYC_Taxi_Gold_Delta.py
│
├── gold/
│   └── README.md
│
├── screenshots/
│   ├── 01-adf-pipeline.png
│   ├── 02-adls-bronze.png
│   ├── 03-bronze-trip-data.png
│   ├── 04-databricks-silver.png
│   ├── 05-adls-silver.png
│   ├── 06-databricks-gold-delta.png
│   └── 07-adls-gold.png
│
└── README.md
