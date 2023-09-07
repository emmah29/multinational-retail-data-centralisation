# Multinational Retail Data Centralisation

This project is the end of the first part of the AICore training program. It demonstrates programming using Python Pandas using multiple Python technologies and skills.

Loading and saving
- Postgres Database 
    - Local and remote
- AWS RDS Database
- PDF

Packages 
- pyYAML ![pyYAML](https://img.shields.io/badge/pyYAML-brightgreen.svg)
- tabula-py ![tabula-py](https://img.shields.io/badge/tabula-py-brightgreen.svg)
- time_uuid ![tabula-py](https://img.shields.io/badge/time-uuid-brightgreen.svg)
- re ![re](https://img.shields.io/badge/re-brightgreen.svg)
- boto3 ![boto3](https://img.shields.io/badge/boto3-brightgreen.svg)

Pandas Data Cleaning
Check and remove...
- Null Rows
- Column data not in a list
- Column data that isn't a valid date
- Not numeric
- Numerics in a range
- Using a function
- Unwanted characters

## Description
The system downloads, cleans and saves locally
- User Data to dim_users
- Card Data to dim_card_details
- Store Data to dim_store_details
- Product Details to dim_products
- Orders Table to orders_table
- Date Events Data to dim_date_times

### Classes
- DataCleaning
- - This is a utility class
- DataExtractor
- -  

## Motivation
This provides AICore with evidence of my skills

## Learnt
It was wonderful to have the opportunity to use the skills that have been learned. 

## Installation
Download the folder multinational-retail-data-centralisation from GIT. The following files are used by the system.
- data_cleaning.py
- data_extraction.py
- database_utils.py
- db_creds.yaml
- local_db_creds.yaml
- run.py

## Usage
Run program <b>run.py</b> to run the system.

## Contributing
This is a private project. No contributions are requested.

## License
This is a public project. No license is required. 