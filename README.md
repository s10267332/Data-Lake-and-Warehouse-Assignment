# Data-Lake-and-warehouse-Assignment

## Overview
This repository contains the implementation of a full data engineering pipeline built using Snowflake.  
The assignment is divided into **Part A** and **Part B**, each focusing on different aspects of modern data lake and data warehouse architecture.

The project simulates a real-world enterprise workflow where structured, semi-structured, and unstructured data are ingested, transformed, automated, and optimized for analytical workloads.

---

## Part A — Semi-Structured & Unstructured Data Integration

### Objective
Part A focuses on designing and building a curated dataset by integrating:

- Semi-structured JSON data
- Unstructured media files (images, PDFs, videos)
- Geo-spatial context
- Structured analytical tables

The goal is to simulate a real enterprise ingestion pipeline where raw operational data is transformed into a usable analytical dataset.

### Key Components

#### JSON Data Engineering
- Designed deeply nested JSON datasets (≥5 hierarchy levels)
- Implemented multi-array structures requiring advanced flattening
- Linked master and transaction records using consistent IDs
- Included geo-spatial attributes (coordinates, locations)

#### Snowflake Processing
- Created internal stages for JSON, images, and videos
- Applied file formats for semi-structured ingestion
- Flattened JSON using multi-step CTAS transformations
- Joined structured data with unstructured assets
- Generated scoped URLs for secure media access

#### Curated Dataset
The final output is a clean analytical dataset combining:

- Master records
- Transaction records
- Image references
- Video references
- Enriched contextual attributes

This dataset represents what an analyst, dashboard, or ML pipeline would consume.

### Learning Outcomes
- Handling deep semi-structured hierarchies
- Linking structured + unstructured datasets
- Geo-spatial enrichment
- Data modelling for analytics
- Snowflake staging & flattening workflows

---

## Part B — Automation & Performance Optimization

Part B extends the curated dataset into an automated and optimized enterprise pipeline.

---

### Part B Question 1 — Partitioned External Tables

#### Objective
Improve query performance by partitioning large external datasets.

#### Implementation
- Created external tables partitioned by **Year**, **Quarter**, and **Month**
- Demonstrated partition pruning using Query Profile
- Compared partitioned vs non-partitioned scans
- Showed reduction in bytes scanned and runtime

#### Result
Partitioning significantly reduced compute cost and improved query efficiency without changing output accuracy.

---

### Part B Question 2 — Automated Unstructured Pipeline

#### Objective
Build a fully automated pipeline for processing PDFs using:

- Python UDFs
- Streams
- Tasks
- Directed Acyclic Graph (DAG)

#### Pipeline Architecture

1. **Stage + Directory Table**
   - PDFs stored in Snowflake internal stage
   - Directory tracking enabled

2. **Stream Detection**
   - Stream captures new file events

3. **Task T1 — Metadata Load**
   - Extracts file metadata
   - Writes to raw catalog table

4. **Task T2 — PDF Parsing**
   - Python UDF extracts text and metadata
   - Stores results in processed table

5. **Task T3 — Final Analytical Views**
   - Combines PDF data with curated dataset
   - Attaches aggregated metrics
   - Uses partition pruning

#### Engineering Challenges Solved
- Stream consumption conflicts
- Exploding joins on large external datasets
- Inefficient scans
- DAG reliability issues

Each challenge was resolved through architectural redesign, pre-aggregation, and task isolation.

---

### Performance Demonstration
Query Profile comparison shows:

- Partitioned views scan fewer files
- Lower bytes scanned
- Faster execution time
- Stable recurring performance

---

## Technologies Used

- Snowflake SQL
- Python UDF (PDF parsing)
- Streams & Tasks DAG
- External Tables
- Materialized Views
- Partition Pruning
- Semi-Structured JSON Processing
- Unstructured Data Integration

---

## Conclusion
This project demonstrates a scalable enterprise-grade architecture that integrates:

✔ Structured + unstructured data  
✔ Automated ingestion pipelines  
✔ Performance optimization  
✔ Cost-efficient design  
✔ Secure media handling  
✔ Reliable DAG orchestration  
