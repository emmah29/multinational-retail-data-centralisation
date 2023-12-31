from datetime import datetime, timedelta
import time_uuid
import pandas as pd
import re
import uuid

class DataCleaning:
    '''
        DataCleaning
        This is a utility class that provides methods that clean data
        for specific entities

        Attributes:
            None
    '''

    # This class doesn't have an __init__

    def clean_user_data(data: pd.DataFrame):
        '''
        Cleans the user data

        The following cleaning is done 
        - Remove/Replace columns/rows with NULL values
        - errors with dates
        - incorrectly typed values  
            - email address must have a @
        - rows filled with the wrong information
            - product_code & store_code have been checked.. all are okay

        Parameters: 
            data: Dataframe: The dataset to be cleansed

        Returns: 
            Dataframe: A new dataframe with the cleaned dataset within it
        '''
        valid_countries = ['Germany', 'United Kingdom', 'United States']
        valid_country_code = ['DE', 'GB', 'US']
        # First copy the Dataframe
        cloned_data = data.copy(deep=True)
        # Remove null rows
        cleansed_data = cloned_data.dropna(axis=0)
        # - Remove/Replace data/columns/rows with NULL values
        # - remove invalid dates
        cleansed_data['valid_date_of_birth']  = cleansed_data['date_of_birth'].apply(lambda x: pd.to_datetime(x, format=" %Y-%m-%d", errors='coerce'))
        cleansed_data = cleansed_data.loc [ cleansed_data['valid_date_of_birth'] != pd.NaT ] 
        cleansed_data.drop(['valid_date_of_birth'], axis=1, inplace = True)   
        cleansed_data['valid_join_date']  = cleansed_data['join_date'].apply(lambda x: pd.to_datetime(x, format=" %Y-%m-%d", errors='coerce'))
        cleansed_data = cleansed_data.loc [ cleansed_data['valid_join_date'] != pd.NaT ] 
        cleansed_data.drop(['valid_join_date'], axis=1, inplace = True)   
        # - email address
        cleansed_data['email_at_sign_location'] = cleansed_data['email_address'].str.find('@')
        cleansed_data = cleansed_data.loc [ cleansed_data['email_at_sign_location'] >= 0 ] 
        cleansed_data.drop(['email_at_sign_location'], axis=1, inplace = True)    
        # - country
        cleansed_data =  cleansed_data.loc[ cleansed_data['country'].isin(valid_countries)]
        # - country code
        cleansed_data =  cleansed_data.loc[ cleansed_data['country_code'].isin(valid_country_code)]
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
        # Retain the web store
        #web_store = data.loc[data['store_code'] == 'WEB-1388012W']
        # locate the row to update
        row_index = cleansed_data.loc[cleansed_data['store_code'] == 'WEB-1388012W'].index[0]
        cleansed_data.loc[row_index, 'longitude'] = 0
        cleansed_data.loc[row_index, 'latitude'] = 0
        # Set up valid codes
        valid_store_types = ['Super Store', 'Local', 'Outlet', 'Mail Kiosk', 'Web Portal']
        valid_country_code = ['US', 'GB', 'DE']
        valid_continent = ['America', 'Europe']
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
        cleansed_data.drop(['valid_opening_date'], axis=1, inplace = True)    
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
        # Add the web store back in
        #cleansed_data = pd.concat([cleansed_data, web_store]) 
        
        return cleansed_data
    
    def convert_product_weights(data: pd.DataFrame):
        '''        
        take the products DataFrame as an argument and return the products DataFrame

        - Within the weight column in the DataFrame the weights all have different units.
        These are converted to a decimal value representing their weight in kg. Using 
        a 1:1 ratio of ml to g as a rough estimate for the rows containing ml. 
        All excess characters that represent the weights as a float are removed.

        Parameters: 
            data: Dataframe: The dataset to be cleansed

        Returns: 
            Dataframe: A new dataframe with the cleaned dataset within it
        '''
        # First copy the Dataframe
        cleansed_data = data.copy(deep=True)
        # cleanse the product weight
        cleansed_data['weight'] = cleansed_data['weight'].apply(DataCleaning.weight)
        # New column weight_class
        #          +--------------------------+-------------------+
        #          | weight_class VARCHAR(?)  | weight range(kg)  |
        #          +--------------------------+-------------------+
        #          | Light                    | < 2               |
        #          | Mid_Sized                | >= 2 - < 40       |
        #          | Heavy                    | >= 40 - < 140     |
        #          | Truck_Required           | => 140            |
        #          +----------------------------+-----------------+
        cleansed_data['weight_class'] = cleansed_data['weight'].apply(DataCleaning.weight_class)

        return cleansed_data
    def weight(input_weight: str):
        '''
        Change weights to grammes without kg or g

        Parameters: input_weight: str: A string containing a weight suffixed with kg, g, ml or oz

        This removes the suffix and converts all weights/volumes to grammes. If the weight contains
        a multiplier eg. 10 x 45g, then it handles this. 

        Any conversions that aren't possible due to different units or random characters return NaN 
        so that the row can be identified and removed. 
        '''
        # Remove characters that aren't numeric, x, k, g, m, l, o, z, .
        input_weight = re.sub('[^0123456789xkgmloz.]', '', input_weight)
        # Handle multipliers
        if input_weight.find('x') > 0:
            quantity = float(input_weight[0:input_weight.find('x')])
            alphanumeric_weight = str.strip(input_weight[input_weight.find('x')+1:])
            converted_weight =  DataCleaning.weight(alphanumeric_weight) * quantity
        # Strip kg
        elif input_weight.find('kg') > 0:
            try: 
                converted_weight = float(input_weight.strip('kg'))
            except: 
                converted_weight = float('nan')
        # Strip g, divide by 1000
        elif input_weight.find('g') > 0:
            try:
                converted_weight = float(input_weight.strip('g')) / 1000
            except: 
                converted_weight = float('nan')
         # Strip ml, divide by 1000
        elif input_weight.find('ml') > 0:
            try:
                converted_weight = float(input_weight.strip('ml') ) / 1000
            except: 
                converted_weight = float('nan')
        elif input_weight.find('oz') > 0:
            try:
                converted_weight = float(input_weight.strip('oz') ) * 28 / 1000
            except: 
                converted_weight = float('nan')
        else:
            print(f'Weight {input_weight} not handled')
            converted_weight = float('nan')

        # Convert all to float (catches any without kg or g)
        return float(converted_weight)
    
    def weight_class(weight_in_grammes: int):
        '''
        Converts a weight in grammes into a weight class in the following ranges

                 +--------------------------+-------------------+
                 | weight_class VARCHAR(?)  | weight range(kg)  |
                 +--------------------------+-------------------+
                 | Light                    | < 2               |
                 | Mid_Sized                | >= 2 - < 40       |
                 | Heavy                    | >= 40 - < 140     |
                 | Truck_Required           | => 140            |
                 +----------------------------+-----------------+
        
        Parameters: 
            weight_in_grammes: int: The weight to be converted

        Returns: 
            str: The weight class (Light, Mid-Sized, Heavy, Truck_Required)
        '''
        weight_class = ''
        if weight_in_grammes < 2: 
            weight_class = 'Light'
        elif weight_in_grammes >= 2 and weight_in_grammes < 40:
            weight_class = 'Mid_Sized'
        elif weight_in_grammes >= 40 and weight_in_grammes < 140:
            weight_class = 'Heavy'
        else:
            weight_class = 'Truck_Required'

        return weight_class

    def clean_products_data(data: pd.DataFrame):
        '''        
        Removes any erroneous values from the DataFrame, NULL values or errors with formatting.

        - removes nulls
        - product price - strip £
        - weight - strip units and calculate if necessary
        - category - remove any rows not in a valid category
        - date added - remove any rows  with invalid dates
        - removed - remove any rows not in a valid status

        Parameters: 
            data: Dataframe: The dataset to be cleansed

        Returns: 
            Dataframe: A new dataframe with the cleaned dataset within it
        '''
        valid_categories = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 'food-and-drink', 'diy']
        valid_removed = ['Still_avaliable', 'Removed']
        # First copy the Dataframe
        cleansed_data = data.copy(deep=True)
        # removes nulls
        cleansed_data = cleansed_data.dropna(axis=0)
        # product price - strip £
        cleansed_data['product_price'] = cleansed_data['product_price'].str.strip('£')
        # category - remove any rows not in a valid category
        cleansed_data =  cleansed_data.loc[ cleansed_data['category'].isin(valid_categories)]
        # date added - remove any rows  with invalid dates
        cleansed_data['valid_opening_date'] = cleansed_data['date_added'].apply(lambda x: pd.to_datetime(x, format=" %Y-%m-%d", errors='coerce'))
        cleansed_data = cleansed_data.loc [ cleansed_data['valid_opening_date'] != pd.NaT ] 
        cleansed_data.drop(['valid_opening_date'], axis=1, inplace = True)    
        # removed - remove any rows not in a valid status
        cleansed_data =  cleansed_data.loc[ cleansed_data['removed'].isin(valid_removed)]
        # weight - strip units and calculate if necessary
        cleansed_data = DataCleaning.convert_product_weights(cleansed_data)
        # remove any rows that couldn't be converted
        cleansed_data = cleansed_data.loc [ cleansed_data['weight'] != float('nan')] 
    
        return cleansed_data
        
    def clean_orders_data(data: pd.DataFrame):
        '''        
        Removes any erroneous values from the DataFrame, NULL values and unwanted columns.

        - removes nulls
        - remove - columns first name, last name and 1

        Parameters: 
            data: Dataframe: The dataset to be cleansed

        Returns: 
            Dataframe: A new dataframe with the cleaned dataset within it
        '''
        # First copy the Dataframe
        cleansed_data = data.copy(deep=True)
        # remove - first name and last name and 1
        cleansed_data.drop(['first_name', 'last_name', '1'], axis=1, inplace = True)
        # removes nulls
        cleansed_data = cleansed_data.dropna(axis=0)

        return cleansed_data
    
    def clean_events_data(data: pd.DataFrame):
        '''        
        Removes any erroneous values from the DataFrame, NULL values or errors with formatting.

        - removes nulls
        - remove rows with invalid dates
        - remove rows with invalid timeperiods


        Parameters: 
            data: Dataframe: The dataset to be cleansed

        Returns: 
            Dataframe: A new dataframe with the cleaned dataset within it
        '''
        # Valid Values
        valid_time_periods = ['Evening', 'Morning', 'Midday', 'Late_Hours']
        # First copy the Dataframe
        cleansed_data = data.copy(deep=True)
        # Remove rows with invalid dates
        cleansed_data['concatenated_date_timestamp']  = cleansed_data['year'] + '/' + cleansed_data['month'].str.zfill(2) + '/' + cleansed_data['day'].str.zfill(2) + ' ' + cleansed_data['timestamp'] 
        cleansed_data['valid_date_timestamp']  =  cleansed_data['concatenated_date_timestamp'].apply(lambda x: pd.to_datetime(x, format="%Y/%m/%d %H:%M:%S", errors='coerce'))
        cleansed_data = cleansed_data.loc [ cleansed_data['valid_date_timestamp'] != pd.NaT ] 
        cleansed_data.drop(['valid_date_timestamp', 'concatenated_date_timestamp'], axis=1, inplace = True)  
        # remove rows with invalid timeperiods
        cleansed_data =  cleansed_data.loc[ cleansed_data['time_period'].isin(valid_time_periods)]

        return cleansed_data

    def main():
        print('Nothing in main')

if __name__ == '__main__':
    print('data_cleaning running main')
    DataCleaning.main()
        


