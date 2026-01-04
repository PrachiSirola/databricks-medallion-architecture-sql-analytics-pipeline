# databricks-medallion-architecture-sql-analytics-pipeline
## Overview
This project demonstrates an **end-to-end analytics pipeline** built using **Databricks SQL and notebooks**, following the **Bronze → Silver → Gold (Medallion Architecture)** pattern.

The focus is on converting **raw, inconsistent data** into **clean, business-ready datasets**, while handling real-world issues such as null values, invalid records, negative deltas, and pipeline restarts.

---

## Tech Stack
- Databricks (SQL, Notebooks)
- SQL, Python
- Git & GitHub
- Medallion Architecture (Bronze / Silver / Gold)

---

## Pipeline Summary

### Bronze Layer
- Ingests raw data with no transformations
- Preserves source data for traceability and reprocessing

### Silver Layer
- Removes invalid rows and handles null values
- Normalizes negative deltas
- Standardizes schema and data types
- Rebuilt multiple times to ensure correctness and reliability

### Gold Layer
- Creates aggregated, analytics-ready tables
- Produces clean business metrics for reporting and BI tools

---

## Key Highlights
- Designed and rebuilt a clean **Bronze → Silver → Gold pipeline**
- Implemented robust **data cleaning and validation logic**
- Debugged and resolved repeated pipeline failures
- Built SQL transformations optimized for readability and maintenance
- Delivered analytics-ready datasets for decision-making

---

## Use Case
- Data Analyst / Analytics Engineer portfolio
- Databricks SQL reference project
- Medallion Architecture demonstration

---

## Author
Built as a hands-on Databricks SQL project focused on **real-world data challenges, reliability, and correctness**.
