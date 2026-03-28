# azure-energy-anomaly-pipeline

# Azure Energy Anomaly Pipeline

This project demonstrates an end-to-end data pipeline built using Azure services with data quality, anomaly detection, and governance.

## Architecture

Raw → Silver → Gold → Anomaly + Quarantine

## Key Features

- Data ingestion into ADLS Gen2
- Data quality validation (valid vs invalid split)
- Quarantine layer for rejected data
- Aggregated gold layer (business-ready metrics)
- Anomaly detection for abnormal energy usage
- Pipeline orchestration using Azure Data Factory
- Data governance using Microsoft Purview

## Tech Stack

- Azure Data Factory
- Azure Databricks (PySpark)
- ADLS Gen2
- Microsoft Purview

## Output

- Clean data (Silver)
- Rejected data (Quarantine)
- Aggregated metrics (Gold)
- Anomaly detection results



