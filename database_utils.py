import yaml
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import inspect
from sklearn.datasets import load_iris
import pandas as pd


class DatabaseConnector:
    '''
        DatabaseConneictor
        This is a utility class.

        Attributes:
            engine_instance
    '''

    def __init__(self):
        # attributes
        self.engine_instance = self.init_db_engine()

    def read_db_creds(self):
        '''
        Reads the credentials YAML file and
        returns them as a dictionary.

        Parameters: None
        Returns:
            dict: The credentials from the file
        '''
        with open('db_creds.yaml', 'r') as f:
            items = yaml.safe_load(f)

        return items

    def init_db_engine(self):
        '''
        read the credentials from the return of read_db_creds and initialise
        and returns an sqlalchemy database engine

        Paramaters: None
        Returns:
            sqlalchemy.engine.base.Connection: An sqlalchemy database engine connection

        '''
        credentials = DatabaseConnector.read_db_creds(self)
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
        # and do the connection to get and instance
        instance = engine.execution_options(isolation_level='AUTOCOMMIT').connect()

        return instance

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
        data.to_sql(tablename, self.engine_instance)

    if __name__ == '__main__':
        print('Database_Utils running main')

# connector = DatabaseConnector()
# tables = pd.read_sql_query('''SELECT tablename FROM pg_catalog.pg_tables LIMIT 10''', me.engine_instance)
# print( tables)
