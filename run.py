# import pandas as pd
# from database_utils import DatabaseConnector
# from data_cleaning import DataCleaning
# from data_extraction import DataExtractor
# from tabula.io import read_pdf
# import boto3
# import requests
# from io import StringIO as SIO

def run():
    print('.....................')
    print('Getting the data....')
    import get_data
    print('All data received and cleaned')
    print('.....................')    
    print('Running the queries...')
    import multinational_reports
    print('.....................')
    print('......complete.......')  
    print('.....................')
if __name__ == '__main__':
    print('Running main')
    run()
    

