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
        # Remove null rows
        cleansed_data = cloned_data.dropna(axis=0)
        # Card number must be numeric
        cleansed_data =  cleansed_data[ cloned_data.card_number.str.isnumeric().fillna(False) ]
        # Expiry date must be a valid mm/yy
        #   Expand MM/YY to DD/MM/YY
        cleansed_data['Full_expiry_date'] = cleansed_data['expiry_date'].apply(lambda x: ('01/' + x) ) 
        #   Create a new column that contains either the datetime or NaT if it is an invalid date
        cleansed_data['valid_expiry_date']  = cleansed_data['Full_expiry_date'].apply(lambda x: pd.to_datetime(x, format="%d/%m/%y", errors='coerce'))
        #   Remove the rows that contain NaT (are invalid dates)
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


    def called_clean_store_data(data: pd.DataFrame):
        '''        
        Removes any erroneous values from the pdf, NULL values or errors with formatting.

        - removes nulls
        - address - Remove random letters - hard to identify
        - longitude - Remove non-numerics
        - lat - Largely unpopulated and not clear what it is, nothing removed
        - locality - Remove any locality starting with a number
        - staff numbers - remove non numerics and non integers
        - opening date - remove invalid dates
        - store type - Identify all the valid stores and remove any that aren't in one of these
        - latitude - remove non-numeric & numbers < -90 or > 90. 
        - country code - Identify all the valid stores and remove any that aren't in one of these
        - continent - Identify all the valid stores and remove any that aren't in one of these

        Parameters: 
            data: Dataframe: The dataset to be cleansed

        Returns: 
            Dataframe: A new dataframe with the cleaned dataset within it
        '''
        # First copy the Dataframe
        cleansed_data = data.copy(deep=True)
        # Set up valid codes
        valid_store_types = ['Super Store', 'Local', 'Outlet', 'Mail Kiosk']
        valid_country_code = ['US', 'GB', 'DE']
        valid_continent = ['America', 'Europe']
        # remove nulls
        cleansed_data.dropna(axis=0)
        # address
        # - Remove random letters - hard to identify
        # longitude
        # - Remove non-numerics
        cleansed_data['longitude_numeric'] =  cleansed_data['longitude'].apply(pd.to_numeric, errors='coerce')
        cleansed_data = cleansed_data.loc[ ~cleansed_data['longitude_numeric'].isnull() ] 
        cleansed_data = cleansed_data.loc[ (cleansed_data.longitude.astype(float) > -180) & (cleansed_data.longitude.astype(float) < 180)]
        cleansed_data.drop(['longitude_numeric'], axis=1, inplace = True)
        
        # lat 
        # - Only nulls after removal of bad rows so this will be removed.
        cleansed_data.drop(['lat'], axis=1, inplace = True)
        # locality
        # - Remove any locality starting with a number
        cleansed_data = cleansed_data.loc[ ~cleansed_data['locality'].str[0].str.isdigit() ]
        # staff numbers
        # - remove non numerics and non integers
        cleansed_data['staff_numbers_numeric'] =  cleansed_data['staff_numbers'].apply(pd.to_numeric, errors='coerce')
        cleansed_data = cleansed_data.loc [ ~cleansed_data['staff_numbers_numeric'].isnull() ] 
        cleansed_data.drop(['staff_numbers_numeric'], axis=1, inplace = True)
        # opening date
        # - remove invalid dates
        cleansed_data['valid_opening_date']  = cleansed_data['opening_date'].apply(lambda x: pd.to_datetime(x, format=" %Y-%m-%d", errors='coerce'))
        cleansed_data = cleansed_data.loc [ cleansed_data['valid_opening_date'] != pd.NaT ] 
        print( cleansed_data.loc[['index', 'opening_date','valid_opening_date']] )
        #cleansed_data.drop(['valid_opening_date'], axis=1, inplace = True)    
        # store type
        # - Identify all the valid stores and remove any that aren't in one of these
        cleansed_data =  cleansed_data.loc[ cleansed_data['store_type'].isin(valid_store_types)]
        # latitude
        # - remove non-numeric & numbers < -90 or > 90. 
        cleansed_data['latitude_numeric'] =  cleansed_data['latitude'].apply(pd.to_numeric, errors='coerce')
        cleansed_data = cleansed_data.loc [ ~cleansed_data['latitude_numeric'].isnull() ] 
        cleansed_data = cleansed_data.loc[ (cleansed_data.latitude.astype(float) > -90) & (cleansed_data.latitude.astype(float) < 90)]
        cleansed_data.drop(['latitude_numeric'], axis=1, inplace = True)
        # country code
        # - Identify all the valid stores and remove any that aren't in one of these
        cleansed_data = cleansed_data.loc[ cleansed_data['country_code'].isin(valid_country_code)]
        # continent
        # # - Identify all the valid stores and remove any that aren't in one of these
        cleansed_data = cleansed_data.loc[ cleansed_data['continent'].isin(valid_continent)]
        
        return cleansed_data