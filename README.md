# Zillow Data Pipeline

## Overview
This repository contains DAG logic for facilitating a data pipeline designed to process Zillow property data. It showcases how to efficiently handle rate-limited APIs, daily data ingestion, and data transformations to create actionable datasets for property insights.

I've been running this pipeline and you can find the results of the processing in this dataset I posted on [Kaggle](https://www.kaggle.com/datasets/tonygordonjr/zillow-real-estate-data?select=property_listings.csv).  This Zillow data is being extracted from this [RapidAPI service](https://rapidapi.com/apimaker/api/Zillow.com)

*Disclaimer: This dataset is intended for non-commercial, academic purposes and does not infringe upon Zillow's intellectual property rights. For full details on Zillow's terms, please visit Zillow's Terms of Use.*

## Features
- **Daily Data Ingestion:** Queries Zillow data for properties listed and sold within the last day.
- **Efficient Data Updates:** Logic to merge new data into staging datasets, updating existing records and inserting new ones.
- **Customizable Workflows:** Modular and reusable DAG components that can be adapted for other data pipelines.
- **Built-in Rate Limit Handling:** Ensures compliance with Zillow's strict rate limits while maximizing data collection.

## Use Cases
This pipeline can serve as a reference or starting point for:
- **Real Estate Analytics:** Building dashboards for market trends, property comparisons, and time-on-market metrics.
- **ETL Pipelines:** Automating data collection, transformation, and storage for real estate data.
- **Data Science Projects:** Preparing clean and structured data for machine learning or statistical analysis.

## How It Works

### API Interaction
- Queries Zillow’s endpoints for daily listings and sold properties.
- Handles rate limits to ensure data retrieval within allocated quotas.

### Data Transformation
- Processes raw property data by scraping the JSON response using a variety of functions.
- Each function represents a table that'll be updated in BigQuery.


### Data Storage
- Data is stored into GCS for historical purposes and then copied into a loading dataset in BigQuery.
- Dataform is triggered to clean, model and insert data into reporting datasets for downstream analytics.


### Extensibility
- Designed with modularity in mind, allowing users to adapt components for their own pipelines.
