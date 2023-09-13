# Multinational Retail Data Centralisation

This project is the end of the first part of the AICore training program. It demonstrates programming using Python Pandas using multiple Python technologies and skills.

- Loading and saving
- Postgres Database 
    - Local and remote
- AWS RDS Database
- PDF

## Contents
- [ Packages Used ](#packages)
- [ Overview ](#overview)
- - [ Pandas Data Cleaning ](#pandas)
- - [ System Description ](#description)
- - [ Classes & methods ](#classes)
- [ Motivation ](#motivation)
- - [ What Learnt ](#learnt)
- [ Installation ](#installation)
- [ How to use the system ](#usage)
- [ Contributing ](#contributing)
- [ License ](#license)


## <a name="overview"></a>Overview
### <a name="packages"></a>Packages Used
- ![pyYAML](https://img.shields.io/badge/pyYAML-brightgreen.svg)
- ![tabula-py](https://img.shields.io/badge/tabula-py-brightgreen.svg)
- ![tabula-py](https://img.shields.io/badge/time-uuid-brightgreen.svg)
- ![re](https://img.shields.io/badge/re-brightgreen.svg)
- ![boto3](https://img.shields.io/badge/boto3-brightgreen.svg)
- ![sqlalchemy](https://img.shields.io/badge/sqlalchemy-brightgreen.svg)

### <a name="pandas"></a>Pandas Data Cleaning
Check and remove...
- Null Rows
- Column data not in a list
- Column data that isn't a valid date
- Not numeric
- Numerics in a range
- Using a function
- Unwanted characters

### <a name="description"></a>System Description
The system downloads, cleans and saves locally
- User Data to dim_users
- Card Data to dim_card_details
- Store Data to dim_store_details
- Product Details to dim_products
- Orders Table to orders_table
- Date Events Data to dim_date_times


### <a name="classes"></a>Classes & methods
- DataExtractor
- - read_rds_table
- - retrieve_pdf_data
- - list_number_of_stores
- - retrieve_stores_data
- - extract_from_s3
- DataCleaning
- - clean_user_data
- - uuid1_to_datetime
- - clean_card_data
- - called_clean_store_data
- - convert_product_weights
- - weight
- - clean_products_data
- - clean_orders_data
- - clean_events_data
- DatabaseConnector
- - read_db_creds
- - init_db_engine
- - list_db_tables
- - Upload_to_db


## <a name="motivation"></a>Motivation
This provides AICore with evidence of my skills

### <a name="learnt"></a>What Learnt
It was wonderful to have the opportunity to use the skills that have been learned. 

## <a name="installation"></a>Installation
Download the folder multinational-retail-data-centralisation from GIT. The following files are used by the system.
- data_cleaning.py
- data_extraction.py
- database_utils.py
- db_creds.yaml
- local_db_creds.yaml
- run.py

## <a name="usage"></a>Usage
1. Make sure that AWS CLI is logged in
1. Run program <b>run.py</b> to run the system.

## <a name="contributing"></a>Contributing
This is a private project for certification. Please provide feedback to emmahumphreys@gmail.com but do not make changes. 

## <a name="license"></a>License
This is a public project. No license is required. 