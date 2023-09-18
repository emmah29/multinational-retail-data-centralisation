import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from tabula.io import read_pdf
import boto3
import requests
from io import StringIO as SIO

def run():
    extractor = DataExtractor()
    # Ensure that the tables don't already exist
    extractor.connector.run_sql_file('precheck_tables.sql')
    # Task 3
    print('find the remote Tables')
    remote_tables = extractor.connector.list_db_tables()
    print(remote_tables)
    print('.............................. complete')

    # Get the user data
    print('working on user_data....')
    remote_db_user_data = extractor.read_rds_table(extractor.connector_instance, 'legacy_users').set_index('index')
    extractor.connector.upload_to_db(remote_db_user_data, 'dim_users_before_cleansing')
    cleansed_data = DataCleaning.clean_user_data(remote_db_user_data)
    extractor.connector.upload_to_db(cleansed_data, 'dim_users')
    print('.............................. complete')
    
    # Get the card details
    print('working on card details....')
    pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    pdf_data = extractor.retrieve_pdf_data(pdf_url)
    extractor.connector.upload_to_db(pdf_data, 'dim_card_details_before_cleansing')
    pdf_data_cleaned = DataCleaning.clean_card_data(pdf_data)
    extractor.connector.upload_to_db(pdf_data_cleaned, 'dim_card_details')
    print('.............................. complete')

    # Get the stores data
    print('working on stores data...')
    header_details = {"x-api-key": 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    url_retrieve_a_store = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    url_number_of_stores = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    number_of_stores = extractor.list_number_of_stores(url_number_of_stores, header_details)
    print('Number of stores: ' + str(number_of_stores))
    stores = extractor.retrieve_stores_data(url_retrieve_a_store, url_number_of_stores, header_details)
    extractor.connector.upload_to_db(stores, 'dim_store_details_before_cleansing')
    stores_cleaned = DataCleaning.called_clean_store_data(stores)
    extractor.connector.upload_to_db(stores_cleaned, 'dim_store_details')
    print('.............................. complete')

    # Get the product data
    print('products....')
    client =  boto3.client('s3')
    response = client.get_object(Bucket='data-handling-public', Key='products.csv')
    s3_data = response['Body'].read().decode('utf-8')
    df = pd.read_csv(SIO(s3_data))  
    extractor.connector.upload_to_db(df, 'dim_products_before_cleansing')
    products_cleaned = DataCleaning.clean_products_data(df)
    extractor.connector.upload_to_db(products_cleaned, 'dim_products')
    print('.............................. complete')

    # Get the Orders Data
    print('working on orders data.....')
    list_of_tables_in_database = extractor.connector.list_db_tables()
    print(list_of_tables_in_database)
    # Read the table
    remote_db_user_data = extractor.read_rds_table(extractor.connector_instance, 'orders_table').set_index('index')
    extractor.connector.upload_to_db(remote_db_user_data, 'orders_table_before_cleansing')
    cleansed_data = DataCleaning.clean_orders_data(remote_db_user_data)
    extractor.connector.upload_to_db(cleansed_data, 'orders_table')
    print('.............................. complete')

    # Get the Events Data
    print('working on events data.....')
    remote_json_data = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    extractor.connector.upload_to_db(remote_json_data, 'dim_date_times_before_cleansing')
    cleansed_data = DataCleaning.clean_events_data(remote_json_data)
    extractor.connector.upload_to_db(cleansed_data, 'dim_date_times')
    print('.............................. complete')

    print('creating the star schema...')
    print('Adding primary keys')
    extractor.connector.run_sql_file('dim_users.sql')
    extractor.connector.run_sql_file('dim_store_details.sql')
    extractor.connector.run_sql_file('dim_products.sql')
    extractor.connector.run_sql_file('dim_date_times.sql')   
    extractor.connector.run_sql_file('dim_card_details.sql')   
    extractor.connector.run_sql_file('orders_table.sql')
    print('.............................. complete')    

    print('Running the queries...')
    # How many stores and in which countries
    orders_table = pd.read_sql('orders_table', DataExtractor().connector.local_engine_instance, index_col='date_uuid')
    dim_date_times = pd.read_sql('dim_date_times', DataExtractor().connector.local_engine_instance, index_col='date_uuid')
    dim_products = pd.read_sql('dim_products', DataExtractor().connector.local_engine_instance, index_col='product_code')
    dim_store_details = pd.read_sql('dim_store_details', DataExtractor().connector.local_engine_instance, index_col='store_code')


    print('How many stores and in which countries')
    print(dim_store_details.groupby('country_code')['country_code'].count())
    # Which locations have the most stores
    
    print('Which locations have the most stores')
    print(dim_store_details.groupby('locality')['locality'].count() \
                    .reset_index(name='count') \
                    .sort_values(['count'], ascending=False)  \
                    .head(6) )
    
    print('Which months produce the average highest cost of sales typically')
    orders_with_date = orders_table.join(dim_date_times, lsuffix='_orders', rsuffix='_dates').set_index('product_code')
    orders_with_date_and_price = orders_with_date.join(dim_products, lsuffix='_orders', rsuffix='_prods')
    orders_with_date_and_price['cost_of_sale'] = orders_with_date_and_price['product_price'] * orders_with_date_and_price['product_quantity']
    print(orders_with_date_and_price.groupby('month')['cost_of_sale'].mean() \
        .reset_index(name='mean') \
        .sort_values(['mean'], ascending=False) \
        .head(6)
       )

    #extractor.connector.run_sql_file('dim_users.sql')
    print('.............................. complete')  

if __name__ == '__main__':
    print('Running main')
    run()
    

