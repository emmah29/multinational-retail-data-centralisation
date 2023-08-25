import pandas as pd

stores = pd.read_json('stores.json')
stores.set_index('index')



# Set up valid codes
valid_store_types = ['Super Store', 'Local', 'Outlet', 'Mail Kiosk']
valid_country_code = ['US', 'GB', 'DE']
valid_continent = ['America', 'Europe']
# remove nulls
stores.dropna(axis=0)
# address
# - Remove random letters - hard to identify
# longitude
# - Remove non-numerics
print(1)
stores['longitude_numeric'] =  stores['longitude'].apply(pd.to_numeric, errors='coerce')
stores = stores.loc [ ~stores['longitude_numeric'].isnull() ] 
stores = stores.loc[ (stores.longitude.astype(float) > -180) & (stores.longitude.astype(float) < 180)]
stores.drop(['longitude_numeric'], axis=1, inplace = True)
# lat 
# - Largely unpopulated and not clear what it is
# locality
# - Remove any locality starting with a number
stores = stores.loc[ ~stores['locality'].str[0].str.isdigit() ]
# staff numbers
# - remove non numerics and non integers
stores['staff_numbers_numeric'] =  stores['staff_numbers'].apply(pd.to_numeric, errors='coerce')
stores = stores.loc [ ~stores['staff_numbers_numeric'].isnull() ] 
stores.drop(['staff_numbers_numeric'], axis=1, inplace = True)
# opening date
# - remove invalid dates
stores['valid_opening_date']  = stores['opening_date'].apply(lambda x: pd.to_datetime(x, format=" %Y-%m-%d", errors='coerce'))
stores = stores.loc [ stores['valid_opening_date'] != pd.NaT ] 
stores.drop(['valid_opening_date'], axis=1, inplace = True)    
# store type
# - Identify all the valid stores and remove any that aren't in one of these
stores =  stores.loc[ stores['store_type'].isin(valid_store_types)]
# latitude
# - remove non-numeric & numbers < -90 or > 90. 
stores['latitude_numeric'] =  stores['latitude'].apply(pd.to_numeric, errors='coerce')
stores = stores.loc [ ~stores['latitude_numeric'].isnull() ] 
stores = stores.loc[ (stores.latitude.astype(float) > -90) & (stores.latitude.astype(float) < 90)]
stores.drop(['latitude_numeric'], axis=1, inplace = True)
# country code
# - Identify all the valid stores and remove any that aren't in one of these
stores = stores.loc[ stores['country_code'].isin(valid_country_code)]
# continent
# # - Identify all the valid stores and remove any that aren't in one of these
stores = stores.loc[ stores['continent'].isin(valid_continent)]
print(stores)
print('1: ' + str(len(stores.index)))

