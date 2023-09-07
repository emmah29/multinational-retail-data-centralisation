import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from tabula.io import read_pdf
import boto3
import requests

def run():
    extractor = DataExtractor()
    print('working on user_data....')
    remote_db_user_data = extractor.read_rds_table(extractor.connector_instance, 'orders_table').set_index('index')
    cleansed_data = DataCleaning.clean_user_data(remote_db_user_data)
    extractor.connector.upload_to_db(cleansed_data, 'dim_users')
    print('.... complete')
    
    print('working on pdf_data....')
    pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    pdf_data = extractor.retrieve_pdf_data(pdf_url)
    pdf_data_cleaned = DataCleaning.clean_card_data(pdf_data)
    extractor.connector.upload_to_db(pdf_data_cleaned, 'dim_card_details')
    print('.... complete')

    print('working on stores data...')
    header_details = {"x-api-key": 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    url_retrieve_a_store = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    url_number_of_stores = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    
    number_of_stores = extractor.list_number_of_stores(url_number_of_stores, header_details)
    print('Number of stores: ' + str(number_of_stores))
    stores = extractor.retrieve_stores_data(url_retrieve_a_store, url_number_of_stores, header_details)
    extractor.connector.upload_to_db(stores, 'stores')
    stores_cleaned = DataCleaning.called_clean_store_data(stores)
    extractor.connector.upload_to_db(stores_cleaned, 'dim_store_details')
    print('.... complete')

    # #Task 6
    # df = extractor.extract_from_s3('s3://data-handling-public/products.csv')
    # products_cleaned = DataCleaning.clean_products_data(df)
    # extractor.connector.upload_to_db(products_cleaned, 'dim_products')

    # Task 7
    list_of_tables_in_database = extractor.connector.list_db_tables()
    print(list_of_tables_in_database)
    # Read the table
    remote_db_user_data = extractor.read_rds_table(extractor.connector_instance, 'orders_table').set_index('index')
    print('working on orders data.....')
    cleansed_data = extractor.connector.set_data_types_orders(DataCleaning.clean_orders_data(remote_db_user_data))
    extractor.connector.upload_to_db(cleansed_data, 'orders_table')
    print('.... complete')

    # Task 8 
    print('working on events data.....')
    remote_json_data = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    #cleansed_data = pd.read_csv("date_details.csv")
    cleansed_data = DataCleaning.clean_events_data(remote_json_data)
    extractor.connector.upload_to_db(cleansed_data, 'dim_date_times')
    print('.... complete')

if __name__ == '__main__':
    print('Running main')
    run()
    

