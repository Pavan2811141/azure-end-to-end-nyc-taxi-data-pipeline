# Bronze Layer

The Bronze layer stores the raw NYC Taxi data in Azure Data Lake Storage Gen2.

## Data

- Trip Data
- Trip Type
- Trip Zone

## Storage Format

The raw trip data is stored in Parquet format.

## Data Ingestion

NYC Taxi source data is ingested using Azure Data Factory and stored in the Bronze layer of ADLS Gen2.
