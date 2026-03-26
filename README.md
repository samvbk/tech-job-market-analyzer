# Tech Job Market Analyzer 🚀

An end-to-end Data Engineering capstone project designed to automatically scrape, process, and visualize the technical job market (specifically Data Engineers & Software Engineers). 

## 🏗️ Architecture

![Architecture Diagram](https://raw.githubusercontent.com/placeholder/architecture.png)
*(Placeholder: Add your architecture diagram pointing RapidAPI -> Mage -> GCS -> BigQuery -> dbt -> Looker)*

## 🛠️ Technology Stack
* **Cloud Provider:** Google Cloud Platform (GCP)
* **Infrastructure as Code:** Terraform
* **Orchestration:** Mage (Dockerized)
* **Data Lake:** Google Cloud Storage (GCS)
* **Data Warehouse:** Google BigQuery
* **Transformation:** dbt (Data Build Tool)
* **Visualization:** Looker Studio
* **Language:** Python

## 📊 Pipeline Overview
1. **Extract:** Python scripts pull daily job postings for specific tech roles via the RapidAPI JSearch free API.
2. **Load (Data Lake):** Orchestrated by Mage, raw data is pushed into a GCS bucket partitioned by execution date using PyArrow in Parquet format.
3. **Load (Data Warehouse):** Mage runs a script to mount the GCS lake as an external BigQuery table with Hive partitioning.
4. **Transform:** A containerized dbt project executes on BigQuery, cleaning data in a `staging` layer and building a `fact_job_postings` table that uses BigQuery Regex to extract tech skills (Python, SQL, AWS, Airflow, dbt, etc.) from the descriptions.
5. **Big Data Synergy:** A new module that pulls 1M+ rows of Stack Overflow tag trends (2019-Present) from a BigQuery Public Dataset. This allows for correlation between "What developers are talking about" vs "What companies are hiring for."

## 🚀 Setup Instructions

1. **GCP & Terraform Setup**
   * Create a Service Account in GCP with `Storage Admin` and `BigQuery Admin` roles.
   * Download the JSON key locally.
   * Navigate to the `terraform/` folder, run `terraform init`, `terraform plan`, and `terraform apply`.

2. **Mage Orchestrator**
   * Create an `.env` file at the root with your RapidAPI key: `RAPIDAPI_KEY=your_key`
   * Run `docker compose up -d` to spin up Mage.
   * Open `http://localhost:6789`, create a scheduled pipeline, and paste the Python blocks located in the `/scripts` directory.

3. **dbt Transformations**
   * Navigate to the `dbt_project/` folder.
   * Install BigQuery adapter: `pip install dbt-bigquery`
   * Run models: `dbt build --profiles-dir .`

## 💡 Top Metrics Visualized
- Top In-Demand Tech Skills
- Average Salary by Employment Type (Remote vs On-site)
- Total Open Job Postings Geographical Distribution

---
*Created for the DataTalksClub Data Engineering Zoomcamp*
