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
            endpoint_with_store_number = store_endpoint.format(store_number=store_iteration)
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
        print('Nothing in main')

if __name__ == '__main__':
    print('data_Extraction running main')
    DataExtractor.run()