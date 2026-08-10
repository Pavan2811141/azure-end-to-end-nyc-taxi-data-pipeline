# NYC Taxi End-to-End Data Engineering Pipeline

## Project Overview

This project implements an end-to-end **cloud data engineering pipeline** for processing NYC Taxi trip data using Microsoft Azure.

The pipeline follows a **Bronze-Silver-Gold (Medallion) Architecture**. Azure Data Factory is used for data ingestion, Azure Data Lake Storage Gen2 is used for cloud data storage, and Azure Databricks with PySpark is used for data transformation and processing.

The raw NYC Taxi data is dynamically ingested from an HTTP source into ADLS Gen2, transformed using PySpark in Azure Databricks, and processed through the Silver and Gold layers. The Gold layer uses **Delta Lake and Delta Tables** to provide curated, analytical-ready data for downstream analysis and reporting.

---

## Architecture

### Data Pipeline Flow

The pipeline follows a **Medallion Architecture** consisting of Bronze, Silver, and Gold layers.

```text
NYC Taxi Dataset
       |
       v
Azure Data Factory
       |
       v
ADLS Gen2 - Bronze Layer
       |
       v
Azure Databricks / PySpark
       |
       v
ADLS Gen2 - Silver Layer
       |
       v
Databricks / Delta Lake
       |
       v
ADLS Gen2 - Gold Layer
       |
       v
Power BI
       |
       v
Interactive Dashboard
Layer Overview
Layer	Processing	Output
Bronze	Raw data ingestion from HTTP source using Azure Data Factory	Raw Parquet files in ADLS Gen2
Silver	Data cleaning, filtering, null handling, duplicate removal, and data type transformations using PySpark	Cleaned Parquet files
Gold	Business transformations, aggregations, and analytical dataset creation using Databricks and Delta Lake	Delta Tables
Power BI	Visualization and analysis of curated Gold data	Interactive Dashboard
Technologies Used
Microsoft Azure
Azure Data Factory
Azure Data Lake Storage Gen2
Azure Databricks
PySpark
Apache Spark
Delta Lake
Power BI
GitHub
Pipeline Flow
1. Data Ingestion

Azure Data Factory is used to dynamically ingest NYC Taxi data from an HTTP source into Azure Data Lake Storage Gen2.

The source data is loaded into the Bronze layer in Parquet format.

2. Bronze Layer

The raw NYC Taxi Parquet files are stored in the Bronze layer of ADLS Gen2.

The Bronze layer preserves the source data in its original/raw form and provides a reliable landing zone for downstream processing.

A small representative sample of the raw data is included in this GitHub repository, while the complete dataset is stored in ADLS Gen2.

3. Silver Layer

Azure Databricks reads the Bronze data from ADLS Gen2 using secure Service Principal authentication.

PySpark is used to perform data cleansing and transformation, including:

Data type transformations
Null handling
Duplicate removal
Column transformations
Data filtering
Data quality processing

The transformed data is written to the Silver layer in ADLS Gen2 in Parquet format.

4. Gold Layer

The Silver data is further processed in Azure Databricks to create the Gold layer.

The Gold data is stored using Delta Lake and registered as Delta Tables.

The Gold layer contains curated and analytical-ready datasets for downstream reporting and analysis.

Example:

df_type.write.format("delta") \
    .mode("append") \
    .option("path", f"{gold}/trip_type") \
    .saveAsTable("GOLD_LAYER.trip_type")
5. Power BI

The curated Gold layer is used for analytical reporting and visualization in Power BI.

Power BI provides an interactive view of the processed NYC Taxi data for analysis and reporting.

Data Lake Architecture
ADLS Gen2
│
├── bronze/
│   └── Raw NYC Taxi Data
│
├── silver/
│   └── Transformed and Cleaned Data
│
└── gold/
    └── Curated Delta Data
Data Transformations
Silver Layer
Data type standardization
Null value handling
Duplicate removal
Column transformations
Data filtering
Data quality processing
Gold Layer
Business-level transformations
Data aggregation
Analytical dataset creation
Delta Table creation
Curated data preparation for reporting
Repository Structure
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
│   │   ├── green_tripdata_2023-03.parquet
│   │   ├── green_tripdata_2023-05.parquet
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
Project Screenshots
1. Azure Data Factory Pipeline

Azure Data Factory is used to dynamically ingest NYC Taxi data from the HTTP source and load it into ADLS Gen2.

2. ADLS Gen2 - Bronze Layer

The Bronze layer stores the raw NYC Taxi data in ADLS Gen2.

3. Bronze Sample Data

Representative NYC Taxi Parquet files stored in the Bronze layer.

4. Databricks - Silver Transformation

Azure Databricks and PySpark are used to read the Bronze data and perform data cleansing and transformation.

5. ADLS Gen2 - Silver Layer

The transformed data is stored in the Silver layer of ADLS Gen2.

6. Databricks - Gold Delta Table

The Gold layer is created using Delta Lake. The processed data is written as Delta Tables in Databricks.

7. ADLS Gen2 - Gold Layer

The curated Gold datasets are stored in the Gold layer of ADLS Gen2.

Power BI Dashboard

The curated Gold layer can be used for downstream analytics and reporting in Power BI.

The dashboard provides an interactive view of the processed NYC Taxi data and enables analysis of key taxi trip metrics.

Add your Power BI dashboard screenshot here if available.

Key Features
End-to-end Azure data engineering pipeline
Dynamic HTTP data ingestion using Azure Data Factory
Azure Data Lake Storage Gen2 implementation
Bronze-Silver-Gold / Medallion Architecture
Secure Service Principal authentication
PySpark-based data transformation
Data cleansing and quality processing
Parquet-based Silver layer
Delta Lake implementation
Databricks Delta Tables
Curated analytical Gold layer
Power BI integration
GitHub-based project version control
Security

Sensitive information is not stored in this repository.

The following information has been excluded from GitHub:

Service Principal secrets
Client secrets
Storage account keys
Passwords
Authentication tokens
Connection strings containing credentials

Authentication credentials are configured securely within the Azure environment.

Data Flow Summary
HTTP Source
    |
    v
Azure Data Factory
    |
    v
ADLS Gen2 - Bronze Layer
    |
    | Secure Service Principal Authentication
    v
Azure Databricks
PySpark Transformations
    |
    v
ADLS Gen2 - Silver Layer
    |
    v
Azure Databricks
Delta Lake
    |
    v
ADLS Gen2 - Gold Layer
    |
    v
Delta Tables
    |
    v
Power BI
    |
    v
Interactive Dashboard
Project Outcome

This project demonstrates how raw NYC Taxi data can be transformed into curated analytical datasets using a cloud-based data engineering architecture.

The pipeline provides:

Automated data ingestion
Scalable cloud data storage
Data cleansing and transformation
Data quality processing
Medallion Architecture implementation
Delta Lake-based analytical storage
Curated datasets for reporting and analysis
Power BI integration for visualization
Skills Demonstrated

Data Engineering: ETL, Data Transformation, Data Quality, Medallion Architecture

Azure: Azure Data Factory, ADLS Gen2, Azure Databricks

Big Data: Apache Spark, PySpark, Delta Lake

Data Formats: Parquet, Delta

Analytics: Power BI

Version Control: Git, GitHub

Conclusion

This project demonstrates the implementation of an end-to-end cloud data engineering pipeline using Microsoft Azure.

The pipeline covers:

Data ingestion
Cloud data storage
Data cleansing
Data transformation
Data quality processing
Delta Lake implementation
Curated analytical data
Business intelligence integration

The project provides practical hands-on experience with Azure Data Factory, ADLS Gen2, Azure Databricks, PySpark, Apache Spark, Delta Lake, Power BI, and GitHub.

Author

Pavan Kolte

Data Engineering | Azure | Databricks | PySpark | SQL
