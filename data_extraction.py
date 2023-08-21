import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
#import tabula
import requests


class DataExtractor:
    '''
        DataExtractor

        This is a utility class.

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
        dfs = tabula.read_pdf(url, pages='all')
        # Concatenate the list into one Dataframe and return
        return pd.concat(dfs)
    
    def list_number_of_stores(self, endpoint: str, header_credentials: dict):
        '''
        Returns the number of stores to extract

        Parameters: 
            url: str: The url of the API
            headerdictionary: str: header dictionary
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

    def retrieve_stores_data(self, endpoint: str, header_credentials: dict):
        '''
        Returns store data for all the stores

        Parameters: 
            url: str: The url of the API
            headerdictionary: str: header dictionary
        Returns: 
            Object: The data within the pdf
        '''
        # Make multiple calls, one for each store to the API, and put the results into a Dataframe
        # Each store has data in a dictionary. These will be concatenated into a single dictionary
        for store_iteration in range(450):
            # Build the endpoint
            endpoint_with_store_number = endpoint.format(store_number=store_iteration+1)
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
             
def run():
    extractor = DataExtractor()
    data = extractor.read_rds_table(extractor.connector_instance, 'orders_table').set_index('index')
    cleansed_data = DataCleaning.clean_user_data(data)
    extractor.connector.upload_to_db(cleansed_data, 'dim_users')
    print(cleansed_data.head()) 

    # retrieve the pdf
    # pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    # pdf_data = extractor.retrieve_pdf_data(pdf_url)
    # pdf_data_cleaned = DataCleaning.clean_card_data(pdf_data)
    # extractor.connector.upload_to_db(pdf_data_cleaned, 'dim_card_details')

    header_details = {"x-api-key": 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    url_retrieve_a_store = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    url_number_of_stores = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    
    # number_of_stores = extractor.list_number_of_stores(url_number_of_stores, header_details)
    # print('number of stores ' + str(number_of_stores))
    # stores = extractor.retrieve_stores_data(url_retrieve_a_store, header_details)
    # print('-- and finally -- ')
    # print(stores)

if __name__ == '__main__':
    print('Data_Extraction running main')
    run()

