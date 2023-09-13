from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from tabula.io import read_pdf
import boto3
import requests
import pandas as pd

class DataExtractor:
    '''
        DataExtractor

        This is a utility class that handles data store load and save.

        Attributes:
            engine
    '''

    def __init__(self):
        '''
        Initialize an instance

        Attributes
            database_utils.DatabaseConnector: engine
            sqlalchemy.engine.base.Connection: instance of the engine
        '''
        self.connector = DatabaseConnector()
        self.connector_instance = self.connector.engine_instance

    def read_rds_table(self, database_extractor, tablename):
        '''
        Extract the database table to a pandas DataFrame

        Parameters: 
            database_extractor: sqlalchemy.engine.base.Connection: Instance of an engine to the database
            tablename: str: Name of a table to read
        Returns: 
            Dataframe: The data for a table
        '''
        # Build up the query
        sql_query = 'SELECT * FROM ' + tablename 
        # Get the data and return it
        return pd.read_sql_query(sql_query, database_extractor)

    def retrieve_pdf_data(self, url):
        '''
        Extract data from a pdf to a pandas DataFrame

        Parameters: 
            url: str: The url of the pdf
        Returns: 
            Dataframe: The data within the pdf
        '''
        # Read the pdf into a list of DataFrames
        dfs = read_pdf(url, pages='all')
        # Concatenate the list into one Dataframe and return
        return pd.concat(dfs)
    
    def list_number_of_stores(self, endpoint: str, header_credentials: dict):
        '''
        Returns the number of stores to extract

        Parameters: 
            url: str: The url of the API
            header_credentials: str: header dictionary
        Returns: 
            int: Number of Stores
        '''
        response = requests.get(endpoint, headers=header_credentials)
        if response.status_code == 200:
        # Access the response data as JSON
            data = response.json()
            number_of_stores = data['number_stores']
            return number_of_stores
        else:    
            return 'API Call failed: Status Code: ' + str(response.status_code)

    def retrieve_stores_data(self, 
                            store_endpoint: str, 
                            number_of_stores_endpoint: str,
                            header_credentials: dict):
        '''
        Returns store data for all the stores

        Parameters: 
            store_endpoint: str: The url of the API that gives store data
            number_of_stores_endpoint: str: The url of the API that gives the number of stores
            header_credentials: str: header dictionary
        Returns: 
            Object: The data within the pdf
        '''
        # Make multiple calls, one for each store to the API, and put the results into a Dataframe
        # Each store has data in a dictionary. These will be concatenated into a single dictionary
        # Todo: Drive this from the number of stores retrieved
        number_of_stores = self.list_number_of_stores(number_of_stores_endpoint, header_credentials)
        for store_iteration in range(number_of_stores-1):
            # Build the endpoint
            endpoint_with_store_number = store_endpoint.format(store_number=store_iteration+1)
            # Call the API
            response = requests.get(endpoint_with_store_number, headers=header_credentials)
            if response.status_code == 200:
                # Access the response data as JSON
                if store_iteration == 0:
                    stores_json = response.json()
                    stores = [stores_json]
                else:
                    stores_json = response.json()
                    stores.append(stores_json)
            else:    
                return 'API Call failed: Status Code: ' + str(response.status_code) + ' Iteration: ' + str(store_iteration+1)    
        return pd.DataFrame(stores)
    
    def extract_from_s3(self, s3_address: str):

        '''
        download and extract the information returning a pandas DataFrame

        Parameters: 
            s3_address: str: The S3 address of the dataframe
        Returns: 
            DataFrame: The dataframe 
        '''    
        bundle_start = s3_address.find('//') + 2
        slash_at = s3_address.find('/',bundle_start)
        bucket_name = s3_address[bundle_start: slash_at]
        file_name = s3_address[(slash_at+1):] 
        s3 = boto3.client('s3')
        #s3.download_file(bucket_name, file_name, file_name)

        return pd.read_csv(file_name)
    

             
def run():
    # Task 3
    extractor = DataExtractor()
    print('working on user_data....')
    remote_db_user_data = extractor.read_rds_table(extractor.connector_instance, 'orders_table').set_index('index')
    cleansed_data = DataCleaning.clean_user_data(remote_db_user_data)
    extractor.connector.upload_to_db(cleansed_data, 'dim_users')
    print('.... complete')
    
    # # Task 4 
    # print('working on AWS S3 PDF_data....')
    # pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    # pdf_data = extractor.retrieve_pdf_data(pdf_url)
    # pdf_data_cleaned = DataCleaning.clean_card_data(pdf_data)
    # extractor.connector.upload_to_db(pdf_data_cleaned, 'dim_card_details')
    # print('.... complete')

    # # Task 5
    # print('working on stores data...')
    # header_details = {"x-api-key": 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    # url_retrieve_a_store = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    # url_number_of_stores = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    # number_of_stores = extractor.list_number_of_stores(url_number_of_stores, header_details)
    # print('Number of stores: ' + str(number_of_stores))
    # stores = extractor.retrieve_stores_data(url_retrieve_a_store, url_number_of_stores, header_details)
    # extractor.connector.upload_to_db(stores, 'stores')
    # stores_cleaned = DataCleaning.called_clean_store_data(stores)
    # extractor.connector.upload_to_db(stores_cleaned, 'dim_store_details')
    # print('.... complete')

    # #Task 6
    # df = extractor.extract_from_s3('s3://data-handling-public/products.csv')
    # products_cleaned = DataCleaning.clean_products_data(df)
    # extractor.connector.upload_to_db(products_cleaned, 'dim_products')

    # # Task 7
    # list_of_tables_in_database = extractor.connector.list_db_tables()
    # print(list_of_tables_in_database)
    # # Read the table
    # remote_db_user_data = extractor.read_rds_table(extractor.connector_instance, 'orders_table').set_index('index')
    # print('working on orders data.....')
    # cleansed_data = DataCleaning.clean_orders_data(remote_db_user_data)
    # cleansed_data = DataExtractor.set_data_types_orders(cleansed_data)
    # extractor.connector.upload_to_db(cleansed_data, 'orders_table')
    # print('.... complete')

    # #Task 8 
    # #Read the table

    # print('working on events data.....')
    # remote_json_data = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    # #cleansed_data = pd.read_csv("date_details.csv")
    # cleansed_data = DataCleaning.clean_events_data(remote_json_data)
    # extractor.connector.upload_to_db(cleansed_data, 'dim_date_times')
    # print(cleansed_data)

    print('Nothing currently here')

if __name__ == '__main__':
    print('Data_Extraction running main')
    run()
    

