from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import inspect
from sklearn.datasets import load_iris
import pandas as pd
import yaml


class DatabaseConnector:
    '''
        DatabaseConneictor
        This is a utility class.

        Attributes:
            engine_instance
    '''

    def __init__(self):
        # attributes
        # Launch the remote db engine
        credentials = DatabaseConnector.read_db_creds(self, 'db_creds.yaml')
        self.engine_instance = self.init_db_engine(credentials)
        # Launch the local db engine
        local_credentials = DatabaseConnector.read_db_creds(self, 'local_db_creds.yaml')
        self.local_engine_instance = self.init_db_engine(local_credentials)  
        self.local_engine_instance

    def read_db_creds(self, filename):
        '''
        Reads the credentials YAML file and
        returns them as a dictionary.

        Parameters: None
        Returns:
            dict: The credentials from the file
        '''
        with open(filename, 'r') as f:
            items = yaml.safe_load(f)

        return items

    def init_db_engine(self, credentials):
        '''
        read the credentials and initialise.
        This returns an sqlalchemy database engine

        Paramaters: None
        Returns:
            sqlalchemy.engine.base.Connection: An sqlalchemy database engine connection

        '''
        HOST = credentials['RDS_HOST']
        PASSWORD = credentials['RDS_PASSWORD']
        USER = credentials['RDS_USER']
        DATABASE = credentials['RDS_DATABASE']
        PORT = credentials['RDS_PORT']
        # These are always the same
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        # Create the engine
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        # and do the connection to get and return the instance
        instance = engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        
        return engine.execution_options(isolation_level='AUTOCOMMIT').connect()

    def list_db_tables(self):
        '''
        Returns a list of the tables available within the database

        Parameters: None
        Returns:
            List: The names of the tables in the database
        '''
        inspector = inspect(self.engine_instance)

        return inspector.get_table_names()

    def upload_to_db(self, data: pd.DataFrame, tablename: str):
        '''
        Takes a Pandas DataFrame and table name to upload to as an argument.

        Parameters:
            data: Dataframe: The dataframe to be uploaded
            tablename: str: The name of the table in the database
        Returns:
            None
        '''
        data.to_sql(tablename, self.local_engine_instance, if_exists='replace')

    if __name__ == '__main__':
        print('Database_Utils running main')

# connector = DatabaseConnector()
# tables = pd.read_sql_query('''SELECT tablename FROM pg_catalog.pg_tables LIMIT 10''', me.engine_instance)
# print( tables)
