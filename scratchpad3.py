import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from tabula.io import read_pdf
import boto3
import requests
from io import StringIO as SIO

# client =  boto3.client('s3')
# response = client.get_object(Bucket='data-handling-public', Key='products.csv')
# s3_data = response['Body'].read().decode('utf-8')
# s3_dataframe = pd.read_csv(SIO(s3_data))    

# df = s3_dataframe.groupby('category')['category'].count() \
# .reset_index(name='count') \
# .sort_values(['count'], ascending=False)

#print('Which months produce the average highest cost of sales typically')
#print(stores_cleaned.groupby('month').count())   

orders_table = pd.read_sql('orders_table', DataExtractor().connector.local_engine_instance, index_col='date_uuid')
dim_date_times = pd.read_sql('dim_date_times', DataExtractor().connector.local_engine_instance, index_col='date_uuid')
dim_products = pd.read_sql('dim_products', DataExtractor().connector.local_engine_instance, index_col='product_code')

print('1')
orders_with_date = orders_table.join(dim_date_times, lsuffix='_orders', rsuffix='_dates').set_index('product_code')
orders_with_date_and_price = orders_with_date.join(dim_products, lsuffix='_orders', rsuffix='_prods')
orders_with_date_and_price['cost_of_sale'] = orders_with_date_and_price['product_price'] * orders_with_date_and_price['product_quantity']

print(orders_with_date_and_price.dtypes)
#print(orders_with_date_and_price)

print(orders_with_date_and_price.groupby('month')['cost_of_sale'].mean() \
    .reset_index(name='mean') \
    .sort_values(['mean'], ascending=False) \
    .head(6)
       )

# The company is looking to increase its online sales.
# They want to know how many sales are happening online vs offline.
# Calculate how many products were sold and the amount of sales made for online and offline purchases.
# You should get the following information:
# +------------------+-------------------------+----------+
# | numbers_of_sales | product_quantity_count  | location |
# +------------------+-------------------------+----------+
# |            26957 |                  107739 | Web      |
# |            93166 |                  374047 | Offline  |
# +------------------+-------------------------+----------+
