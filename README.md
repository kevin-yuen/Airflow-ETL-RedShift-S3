# Airflow ETL Pipeline: Amazon S3 → Amazon Redshift Data Warehouse

## Overview

This project demonstrates the design and implementation of an end-to-end ETL pipeline using **Apache Airflow**, **Amazon S3**, and **Amazon Redshift**. The pipeline ingests semi-structured JSON data from Amazon S3, stages the data in Amazon Redshift, transforms it into a star schema, and performs automated data quality validation throughout the workflow.

The project emphasizes modular pipeline design, reusable custom Airflow operators, configurable execution modes, and production-oriented engineering practices.

---

## Architecture

```
Amazon S3
     │
     ▼
Staging Tables (Redshift)
     │
     ▼
Data Quality Validation
     │
     ▼
Fact Table
     │
     ▼
Data Quality Validation
     │
     ▼
Dimension Tables
     │
     ▼
Final Data Quality Validation
```

---

## Features

* End-to-end orchestration using Apache Airflow
* Automated ingestion of JSON files from Amazon S3
* Amazon Redshift staging, fact, and dimension tables
* Custom Airflow operators for reusable pipeline components
* Configurable table loading strategies
* Configurable append or truncate-insert loading for dimension tables
* Configurable drop-and-rebuild mode for warehouse initialization
* Automated data quality validation between pipeline stages
* Modular SQL helper classes
* Reusable helper utilities for configuration and Redshift variables
* Comprehensive task logging and failure handling

---

## Technology Stack

* Python
* Apache Airflow
* Amazon Redshift
* Amazon S3
* PostgreSQL Hook
* Boto3
* SQL

---

## Data Warehouse Design

The warehouse follows a **star schema** consisting of:

### Fact Table

* `fact_songplays`

### Dimension Tables

* `dim_users`
* `dim_songs`
* `dim_artists`
* `dim_time`

Raw data is first loaded into Redshift staging tables before being transformed into analytical tables.

---

## Custom Airflow Operators

This project implements several reusable custom operators.

### CreateStageOperator

* Creates staging tables
* Supports configurable table recreation or truncation
* Executes multiple SQL statements

### StageToRedshiftOperator

* Reads JSON files from Amazon S3
* Supports recursive S3 directory traversal
* Executes Redshift COPY commands
* Performs guardrail validation before staging

### CreateFactOperator

* Creates and loads fact tables
* Supports append-only loading strategy
* Designed for scalable fact table ingestion

### CreateDimensionOperator

* Creates and loads dimension tables
* Supports:

  * Append mode
  * Truncate-insert mode
  * Optional table recreation

### DataQualityOperator

Performs automated validation including:

* Table contains data
* Expected row counts
* Primary key uniqueness
* Null checks on critical columns
* Fact-to-dimension referential integrity validation

Pipeline execution stops immediately if any validation fails.

---

## Data Quality Checks

The pipeline validates data after each major loading phase.

### Staging Validation

* S3 objects exist before loading
* Staging tables contain data after COPY

### Fact Table Validation

* Expected number of records loaded
* Primary key uniqueness
* Null checks on critical columns

### Dimension Table Validation

* Row count validation
* Primary key uniqueness
* Null checks
* Foreign key integrity against the fact table

These validations help ensure that downstream analytics are built on trustworthy data.

---

## Project Structure

```
dags/
│
├── final_project.py
│
├── helpers/
│   ├── sql_queries.py
│   ├── redshift_variable_manager.py
│   ├── table_helper.py
│   └── ...
│
├── operators/
│   ├── create_stage.py
│   ├── stage_redshift.py
│   ├── create_fact.py
│   ├── create_dimension.py
│   └── data_quality.py
```

---

## Pipeline Configuration

The DAG supports configurable execution modes.

### Warehouse Rebuild

```python
params = {
    "drop_tables": False
}
```

When enabled, staging, fact, and dimension tables are dropped and recreated before loading.

### Dimension Loading Mode

Supported loading strategies include:

* Append
* Truncate-Insert

This allows the pipeline to simulate different warehouse loading patterns.

---

## Engineering Highlights

This project demonstrates:

* ETL pipeline orchestration
* Object-oriented Python design
* Custom Airflow operator development
* Reusable helper modules
* SQL transformation design
* Star schema data modeling
* Data quality framework implementation
* Error handling and pipeline guardrails
* Cloud data warehouse integration
* Modular, maintainable project architecture

---

## Learning Outcomes

Through this project I gained practical experience in:

* Building production-style ETL pipelines
* Designing dimensional data models
* Developing reusable Airflow operators
* Implementing automated data quality validation
* Working with Amazon S3 and Amazon Redshift
* Applying modular software engineering principles to data engineering workflows

---

## Author

**Kevin Yuen**

Data Engineer with experience designing data pipelines, cloud-based ETL workflows, and data warehouse solutions using Python, SQL, Apache Airflow, and Amazon Web Services.
