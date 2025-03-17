# Project Name: Intermate Automated ETL Pipeline

## Overview
This project is an automated ETL pipeline that extracts data from two CSV file sources, transforms it by cleaning and enriching the data, firstly saves it to a GCS bucket then and loads it into a Big query database. The pipeline is built using Python, DBT, Airflow and the pandas library, and it is designed to be easily extensible and customizable.

## Getting Started
### Installation

1) Clone the repository:
git clone https://github.com/Victoloporsche/automated_etl_pipeline.git
2) Navigate to the project directory:
cd automated_etl_pipeline
3) Install the dependencies:
`pip install -r requirements.txt`
4) Set up the environment variables:
.env.local file in the project root with:
`project=YOUR GOOGLE PROJECT ID
bucket_name=YOUR GOOGLE CLOUD STORAGE BUCKET NAME,
shopify_table=YOUR BIGQUERY SHOPIFY TABLE NAME,
facebook_table=YOUR BIGQUERY FACEBOOK ADS TABLE NAME,
clean_dataset=YOUR BIGQUERY CLEAN DATASET NAME`

## Run tests

1) Run the extraction phase test with test_extract.py 
2) Run the loading phase test with test_load_to_biqquery.py
2) To run DBT test navigate go to the dbt_project and run: dbt test

## Run the autmated pipeline with airflow

1) Navigate to the airflow_project directory
2) Activate the virtual environment
3) Initialize the database with: airflow db init
4) Run the scheduler with: airflow scheduler
5) Run the airflow webserver with: airflow webserver -p 8080


## Visualization
The data is visualized using Looker Studio which connects to the facts bigquery tables