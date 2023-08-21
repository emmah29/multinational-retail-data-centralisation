import uuid
from datetime import datetime, timedelta
import time_uuid
import pandas as pd


class DataCleaning:
    '''
        DataCleaning
        This is a utility class.

        Attributes:
            None
    '''

    # This class doesn't have an __init__

    def clean_user_data(data: pd.DataFrame):
        '''
        Cleans the user data

        The following cleaning is done 
        - Remove/Replace columns/rows with NULL values
            - first_name: Replace null with 'unknown'
            - last_name: Replace null with 'unknown'
            - 1: Remove column
        - errors with dates
            - Convert UUID to date
            - Change type of column date_uuid to date
        - incorrectly typed values  
            - Credit card numbers can be 8 to 19 characters long, these are 9 to 19.
        - rows filled with the wrong information
            - product_code & store_code have been checked.. all are okay

        Parameters: 
            data: Dataframe: The dataset to be cleansed

        Returns: 
            Dataframe: A new dataframe with the cleaned dataset within it
        '''
        # First copy the Dataframe
        cleansed_data = data.copy(deep=True)
        # - Remove/Replace data/columns/rows with NULL values
        #     - first_name: Replace null with 'unknown'
        cleansed_data['first_name'].fillna('unknown', inplace = True)
        #     - last_name: Replace null with 'unknown'
        cleansed_data['last_name'].fillna('unknown', inplace = True)
        #     - 1: Remove column
        cleansed_data.drop(['1'], axis=1, inplace = True)
        # - errors with dates
        #     - Convert UUID to date
        #cleansed_dataset['date_timestamp'] = cleansed_dataset['date_uuid'].apply(lambda x: self.uuid1_to_datetime(x))
        return cleansed_data
       
    def uuid1_to_datetime(date_uuid_timestamp):
        ''' 
        Converts a date in UUID format to a datetime

        Parameters: 
            date_uuid_timestamp: str: The date in uuid format as a string
        
        Returns: 
            datetime: The date in datetime format
        '''
        date_uuid_uuid = uuid.UUID(date_uuid_timestamp)
        return time_uuid.TimeUUID(bytes=date_uuid_uuid.bytes).get_datetime() 

    def clean_card_data(data: pd.DataFrame):
        '''        
        Removes any erroneous values from the pdf, NULL values or errors with formatting.

        - Remove null values
        - Card number must be numeric
        - Expiry date must be a valid mm/yy
        - Date payment confirmed must be a valid date yyyy-mm-dd

        Parameters: 
            data: Dataframe: The dataset to be cleansed

        Returns: 
            Dataframe: A new dataframe with the cleaned dataset within it
        '''
        # First copy the Dataframe
        cloned_data = data.copy(deep=True)
        print('cloned_data ' + str(type(cloned_data)))
        # Remove null rows
        cleansed_data = cloned_data.dropna(axis=0)
        # Card number must be numeric
        cleansed_data =  cleansed_data[ cloned_data.card_number.str.isnumeric().fillna(False) ]
        # Expiry date must be a valid mm/yy
        #   Expand MM/YY to DD/MM/YY
        cleansed_data['Full_expiry_date'] = cleansed_data['expiry_date'].apply(lambda x: ('01/' + x) ) 
        #   Create a new column that contains either the datetime or NaT if it is an invalid date
        cleansed_data['valid_expiry_date']  = cleansed_data['Full_expiry_date'].apply(lambda x: pd.to_datetime(x, format="%d/%m/%y", errors='coerce'))
        #   Remove the rows that containt NaT (are invalid dates)
        cleansed_data = cleansed_data[  cleansed_data.valid_expiry_date != pd.NaT]
        #   Tidy up
        cleansed_data.drop(['Full_expiry_date', 'valid_expiry_date'], axis=1, inplace = True)
        # Date payment confirmed must be a valid date yyyy-mm-dd
        #   Create a new column that contains either the datetime or NaT if it is an invalid date
        cleansed_data['valid_date_payment_confirmed']  = cleansed_data['date_payment_confirmed'].apply(lambda x: pd.to_datetime(x, format="%Y-%m-%d", errors='coerce'))
        #   Remove the rows that containt NaT (are invalid dates)
        cleansed_data = cleansed_data[  cleansed_data.valid_date_payment_confirmed != pd.NaT]
        #   Tidy up
        cleansed_data.drop(['valid_date_payment_confirmed'], axis=1, inplace = True)
       
        return cleansed_data
