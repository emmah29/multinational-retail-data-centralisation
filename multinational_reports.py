import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from tabula.io import read_pdf
import boto3
import requests
from io import StringIO as SIO

extractor = DataExtractor()
    # Get the latest tables
orders_table = pd.read_sql('orders_table', DataExtractor().connector.local_engine_instance, index_col='date_uuid')
dim_date_times = pd.read_sql('dim_date_times', DataExtractor().connector.local_engine_instance, index_col='date_uuid')
dim_products = pd.read_sql('dim_products', DataExtractor().connector.local_engine_instance, index_col='product_code')
dim_store_details = pd.read_sql('dim_store_details', DataExtractor().connector.local_engine_instance, index_col='store_code')
dim_users = pd.read_sql('dim_users', DataExtractor().connector.local_engine_instance, index_col='user_uuid')
# Start the queries
##
# How many stores and in which countries
print('How many stores and in which countries')
print(dim_store_details.groupby('country_code')['country_code'].count())
print('..............................')
#
#
# Which locations have the most stores
print('Which locations have the most stores')
print(dim_store_details.groupby('locality')['locality'].count() \
                .reset_index(name='count') \
                .sort_values(['count'], ascending=False)  \
                .head(6) )
print('..............................')
#
#
print('Which months produce the average highest cost of sales typically')
orders_with_date = orders_table.join(dim_date_times, lsuffix='_orders', rsuffix='_dates').set_index('product_code')
orders_with_date_and_price = orders_with_date.join(dim_products, lsuffix='_orders', rsuffix='_prods')
orders_with_date_and_price['cost_of_sale'] = orders_with_date_and_price['product_price'] * orders_with_date_and_price['product_quantity']
print(orders_with_date_and_price.groupby('month')['cost_of_sale'].mean() \
    .reset_index(name='mean') \
    .sort_values(['mean'], ascending=False) \
    .head(6)
    )
print('..............................')
#
#
print('How many sales are coming from online')
cloned_orders_table = orders_table.copy(deep=True)
cloned_orders_table.set_index('store_code', inplace=True)
cloned_orders_table['location'] = orders_table['store_code'].apply(lambda x: 'Web' if x[0:3] == 'WEB' else 'Offline')
grouped_data = cloned_orders_table.groupby('location') 
report = grouped_data['product_quantity'].agg(['sum', 'count'])
report.columns.values[0] = "product_quantity_count"
report.columns.values[1] = "numbers_of_sales"
print(report)
print('..............................')
#
#
print('What percentage of sales are come through each type of store')
orders_with_store = cloned_orders_table.join(dim_store_details, lsuffix='_orders', rsuffix='_stores').set_index('product_code')
orders_with_store_and_price = orders_with_store.join(dim_products, lsuffix='_orders', rsuffix='_prods')
orders_with_store_and_price['cost_of_sale'] = orders_with_store_and_price['product_price'] * orders_with_store_and_price['product_quantity']         
total = orders_with_store_and_price['cost_of_sale'].sum()
grouped_data = orders_with_store_and_price.groupby('store_type')
report = grouped_data['cost_of_sale'].agg(['sum'])
report['percentage_total(%)'] = ((report['sum'] * 10000) // total) / 100
report.columns.values[0] = "total_sales"
print(report)
print('..............................')
#
#
print('Which month has produced the highest cost of sales')
grouped_data = orders_with_date_and_price.groupby(['year', 'month'])
report = grouped_data['cost_of_sale'].sum()  \
                    .reset_index(name='sum') \
                    .sort_values(['sum'], ascending=False)  \
                    .head(12)
report.columns.values[2] = "total_sales"
print(report)
print('..............................')
#
#
print('What is our staff headcount')
grouped_data = dim_users.groupby('country_code')
report = grouped_data['country'].count()
print(report)
#
#
print('Which German store is selling the most')
german_data = orders_with_store_and_price.loc[orders_with_store_and_price['country_code'] == 'DE']
grouped_data = german_data.groupby(['country_code', 'store_type'])
report = grouped_data['cost_of_sale'].sum()
print(report)
print('..............................')
#
#
print('How quickly the company is making sales')
orders_with_date = orders_table.join(dim_date_times, lsuffix='_orders', rsuffix='_dates').set_index('product_code')
orders_with_date_and_price = orders_with_date.join(dim_products, lsuffix='_orders', rsuffix='_prods')
orders_with_date_and_price['cost_of_sale'] = orders_with_date_and_price['product_price'] * orders_with_date_and_price['product_quantity']

# # Add the datetime as a datetimestamp
#cloned_orders_table = orders_table.copy(deep=True)
# Add the datetime as a datetimestamp
orders_with_date_and_price['order_datetimestamp_char'] =  \
        orders_with_date_and_price['year'] + '/' + \
        orders_with_date_and_price['month'].str.zfill(2) + '/' + \
        orders_with_date_and_price['day'].str.zfill(2)  + ' ' + \
        orders_with_date_and_price['timestamp']
orders_with_date_and_price['order_datetimestamp']  =  \
        orders_with_date_and_price['order_datetimestamp_char'].apply(lambda x: pd.to_datetime(x, format="%Y/%m/%d %H:%M:%S", errors='coerce'))
# Get the dataframe in order if the datetimestamp
orders_with_date_and_price.sort_values('order_datetimestamp', inplace=True)
# Add the last datetime to the DataFrame
orders_with_date_and_price['order_datetimestamp_char_previous_day'] =  \
        orders_with_date_and_price['year'].shift(1) + '/' + \
        orders_with_date_and_price['month'].shift(1).str.zfill(2) + '/' + \
        orders_with_date_and_price['day'].shift(1).str.zfill(2)  + ' ' + \
        orders_with_date_and_price['timestamp'].shift(1)
orders_with_date_and_price['order_datetimestamp_previous_day']  =  \
        orders_with_date_and_price['order_datetimestamp_char_previous_day'].apply(lambda x: pd.to_datetime(x, format="%Y/%m/%d %H:%M:%S", errors='coerce'))
# Calculate the difference
# - the first record will have a 0 (fill_value=0) which will where it tries to compare with nothing 
# - this record needs to be removed so that it doesn't affect the average
# - - create an identifier
orders_with_date_and_price['last_row'] = orders_with_date_and_price['timestamp'].shift(1, fill_value=0)
# - - create a new dataframe without the first (null) row
orders_with_date_and_price_without_null = orders_with_date_and_price.loc [ orders_with_date_and_price['last_row'] != 0]
# - - now create the difference column
orders_with_date_and_price_without_null['time_between_orders'] = \
        orders_with_date_and_price_without_null.apply(lambda row: row['order_datetimestamp'] - row['order_datetimestamp_previous_day'], axis=1)
# It is now possible to create the report
grouped_data = orders_with_date_and_price_without_null.groupby('year')
report = grouped_data['time_between_orders'].mean()
print(report)
print('..............................')
