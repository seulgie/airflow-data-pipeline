# Data Pipelines with Airflow

## Project Overview
This project implements an ETL pipeline using Apache Airflow to automate the extraction, transformation, and loading of data from AWS S3 to Amazon Redshift for a fictional music streaming company, Sparkify.

## Dataset
- Log Data: `s3://udacity-dend/log_data`
- Song Data: `s3://udacity-dend/song-data`

## ArchitectureThe pipeline consists of the following tasks:
1. Stage Data: Load raw JSON data from S3 into staging tables in Redshift.
2. Transform & Load Fact Table: Populate the `songplays` fact table.
3. Transform & Load Dimension Tables: Populate the `users`, `songs`, `artists`, and `time` dimension tables.
4. Run Data Quality Checks: Validate the integrity of the data.

## Operators
- `StageToRedshiftOperator`: Copies data from S3 to Redshift staging tables.
- `LoadFactOperator`: Loads fact table using SQL transformations.
- `LoadDimensionOperator`: Loads dimension tables (supports truncate-insert mode).
- `DataQualityOperator`: Runs SQL-based tests to validate data.

## Prerequisites
1. AWS Setup:
- IAM user with necessary permissions.
- Redshift cluster configured.
- S3 bucket containing the dataset.

2. Airflow Setup:
- Connections: aws_credentials and redshift in Airflow.
- Airflow webserver and scheduler running.

**This is the final project of Udacity Data Engineering Nanodegree.**
