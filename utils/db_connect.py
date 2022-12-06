import psycopg2 
from sqlalchemy import create_engine
import pandas as pd
import io
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from utils.config import config
import numpy as np
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
from psycopg2.errors import *


class Fundamentus_DB():

    def __init__(self) -> None:

        #Config function to read in the database connection parameters
        params = config()   
        self.params = params

        #Connecting to database with connection parameters
        conn = psycopg2.connect(**params)
        
        #Isolation level so transactions take effect
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 
        
        #Setting cursor
        cur = conn.cursor()
        
        #Query to list databases to check which exist
        cur.execute('''
                    SELECT datname 
                    FROM pg_database;
                    ''')

        dbs = cur.fetchall()

        #Passing database names to list
        dbs = [c[0] for c in dbs]
        
        
        #Creates databases if they do not already exist in database

        if 'fundamentus_results' not in dbs:
            cur.execute(''' CREATE DATABASE FUNDAMENTUS_RESULTS''')

        if 'fundamentus_balancesheets' not in dbs:
            cur.execute(''' CREATE DATABASE FUNDAMENTUS_BALANCESHEETS ''')

        if 'fundamentus_events' not in dbs:
            cur.execute(''' CREATE DATABASE FUNDAMENTUS_EVENTS ''')

        if 'fundamentus_company_info' not in dbs:
            cur.execute(''' CREATE DATABASE FUNDAMENTUS_COMPANY_INFO ''')


        #Closes connection
        conn.close()
        
    def new_info_table(self, dataframe:pd.DataFrame, ticker:str):
        """
        Inserts table with stock info to the general information
        database

        :param dataframe: Dataframe
        :type dataframe: pd.DataFrame
        :param ticker: stock ticker to use as name of the table
        :type ticker: str

        """     

        #Lower ticker name to lowercase   
        ticker= ticker.lower()

        #Getting the connection parameters to the PostgreSQL DB
        params = self.params

        #Params to connect to DB
        user = params['user']
        password = params['password']
        port = params['port']
        host = params['host']

        #Changing the database to insert the table
        database = 'fundamentus_company_info'

        #SQL Alchemy engine
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

        #Asserting the dataframe fits the expected data types
        self.__assert_info_schema(dataframe)

        #Transforming DataFrame to Table in database
        dataframe.head(0).to_sql(ticker, engine, if_exists='fail',index=False)

        #Get connection and cursor to execute the insert
        conn = engine.raw_connection()
        cur = conn.cursor()

        #IO class to read the dataframe in a string and transfer to DB
        output = io.StringIO()
        dataframe.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        
        #Execute the upload
        cur.copy_from(output, ticker)     


        try:
            #Add constraints
            cur.execute(f''' ALTER TABLE {ticker} ADD CONSTRAINT {ticker}_date_unique UNIQUE ("Data Coletado"); ''')
        except DuplicateTable: pass

        try:
            #Add constraints
            cur.execute(f''' ALTER TABLE {ticker} ALTER COLUMN "Papel" SET NOT NULL; ''')
        except DuplicateTable: pass
            

        conn.commit()
        conn.close()
        engine.dispose()
      
    def new_events_table(self, dataframe:pd.DataFrame, ticker:str):
        """
        Adds new table with stock company events to the DB

        :param dataframe: Dataframe with company events
        :type dataframe: dict
        :param ticker: stock ticker to use as name of the table
        :type ticker: str
        """        

        #Change ticker name to lower case
        ticker = ticker.lower()
        #Getting the connection parameters to the PostgreSQL DB
        params = self.params
        
        #Params to connect to DB
        user = params['user']
        password = params['password']
        port = params['port']
        host = params['host']

        #Changing the database to insert the table
        database = 'fundamentus_events'
        
        #SQL Alchemy engine
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

        #Transform the text in te column to Datetime
        dataframe['Data'] = pd.to_datetime(dataframe['Data'])

        #Asserting the dataframe fits the expected data types
        self.__assert_events_schema(dataframe)

        #Transforming DataFrame to Table in database
        dataframe.head(0).to_sql(ticker, engine, if_exists='fail',index=False)
        

        #Get connection and cursor to execute the insert
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.execute('ROLLBACK')
        

        #IO class to read the dataframe in a string and transfer to DB
        output = io.StringIO()
        dataframe.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        
        #Execute the upload
        cur.copy_from(output, ticker)   
       
        
        
        #No unique date constraint, because there is the possibility of duplicate dates
        #cur.execute(f''' ALTER TABLE {ticker} ADD CONSTRAINT date_unique UNIQUE ("Data","Descrição"); ''')
       

        try:
            #Add constraints
            cur.execute(f''' ALTER TABLE {ticker} ALTER COLUMN "Data" SET NOT NULL; ''')
        except DuplicateTable: pass

        try:
            #Add constraints
            cur.execute(f''' ALTER TABLE {ticker} ALTER COLUMN "Descrição" SET NOT NULL; ''')
        except DuplicateTable: pass
            


        conn.commit()
        conn.close()
        engine.dispose()

    def new_results_table(self, dataframe:pd.DataFrame, ticker: str):
        """Adds new table with company results to the DB

        :param dataframe: dataframe with results data
        :type dataframe: pd.DataFrame
        :param ticker: stock ticker to use as name of the table
        :type ticker: str
        """        
        
        

        dataframe = dataframe.fillna(0)
        #Change ticker name to lower case
        ticker = ticker.lower()
        
        #Getting the connection parameters to the PostgreSQL DB
        params = self.params
        
        #Params to connect to DB
        user = params['user']
        password = params['password']
        port = params['port']
        host = params['host']

        #Changing the database to insert the table
        database = 'fundamentus_results'
        
        #SQL Alchemy engine
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

        #Rename column that contains the dates to Date
        dataframe.rename(columns={'Unnamed: 0':'Data'}, inplace=True)

        #Transform the text in te column to Datetime
        dataframe['Data'] = pd.to_datetime(dataframe['Data'])

        dataframe = dataframe.drop_duplicates('Data', keep='last')

        #Asserting the dataframe fits the expected data types
        self.__assert_balance_schema(dataframe)

        #Transforming DataFrame to Table in database
        try: dataframe.head(0).to_sql(ticker, engine, if_exists='fail',index=False)
        except ValueError: 
            engine.raw_connection().close()
            raise DuplicateTable

        #Get connection and cursor to execute the insert
        conn = engine.raw_connection()
        cur = conn.cursor()

        #IO class to read the dataframe in a string and transfer to DB
        output = io.StringIO()
        dataframe.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        
        #Execute the upload
        cur.copy_from(output, ticker)

        
        try:
            #Add constraints
            cur.execute(f''' ALTER TABLE {ticker} ADD CONSTRAINT {ticker}_date_unique UNIQUE ("Data"); ''')
        
        #Rollback in case of failed transaction and retry
        except InFailedSqlTransaction:
            cur.execute(f'''ROLLBACK''')
            cur.execute(f''' ALTER TABLE {ticker} ALTER COLUMN "Data" SET NOT NULL; ''')

        try:
            #Add constraints
            cur.execute(f''' ALTER TABLE {ticker} ALTER COLUMN "Data" SET NOT NULL; ''')

        #Rollback in case of failed transaction and retry
        except InFailedSqlTransaction: 
            cur.execute(f'''ROLLBACK''')
            cur.execute(f''' ALTER TABLE {ticker} ALTER COLUMN "Data" SET NOT NULL; ''')
        
        #Commit
        conn.commit()
        
        #Close and finish connection to db
        conn.close()
        engine.dispose()
            
    def new_balancesheets_table(self, dataframe:pd.DataFrame, ticker: str):
        """Adds table with company balance sheet to DB

        :param dataframe: dataframe with balance sheet data
        :type dataframe: pd.DataFrame
        :param ticker: stock ticker to use as name of the table
        :type ticker: str
        """        
        dataframe = dataframe.fillna(0)
        
        #Change ticker name to lower case
        ticker = ticker.lower()

        #Getting the connection parameters to the PostgreSQL DB
        params = self.params
        
        #Params to connect to DB
        user = params['user']
        password = params['password']
        port = params['port']
        host = params['host']

        #Changing the database to insert the table
        database = 'fundamentus_balancesheets'
        
        #SQL Alchemy engine
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

        #Rename column that contains the dates to Date
        dataframe.rename(columns={'Unnamed: 0':'Data'}, inplace=True)

        #Transform the text in te column to Datetime
        dataframe['Data'] = pd.to_datetime(dataframe['Data'])

        dataframe = dataframe.drop_duplicates('Data', keep='last')

        #Asserting the dataframe fits the expected data types
        self.__assert_balance_schema(dataframe)

        #Transforming DataFrame to Table in database
        try:dataframe.head(0).to_sql(ticker, engine, if_exists='fail',index=False)
        except ValueError: 
            engine.raw_connection().close()
            raise DuplicateTable

        #Get connection and cursor to execute the insert
        conn = engine.raw_connection()
        cur = conn.cursor()

        #IO class to read the dataframe in a string and transfer to DB
        output = io.StringIO()
        dataframe.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        
     
        #Execute the upload
        cur.copy_from(output, ticker)

      
        try:
            #Add constraints
            cur.execute(f''' ALTER TABLE {ticker} ADD CONSTRAINT {ticker}_date_unique UNIQUE ("Data"); ''')

        #Rollback in case of failed transaction and retry
        except InFailedSqlTransaction: 
            cur.execute(f'''ROLLBACK''')
            cur.execute(f''' ALTER TABLE {ticker} ADD CONSTRAINT date_unique UNIQUE ("Data"); ''')

        try:
            #Add constraints
            cur.execute(f''' ALTER TABLE {ticker} ALTER COLUMN "Data" SET NOT NULL; ''')

        #Rollback in case of failed transaction and retry
        except InFailedSqlTransaction: 
            cur.execute(f'''ROLLBACK''')
            cur.execute(f''' ALTER TABLE {ticker} ALTER COLUMN "Data" SET NOT NULL; ''')
       
        conn.commit()
        
        conn.close()

        engine.dispose()

    def insert_row_results(self, dataframe:pd.DataFrame, ticker:str):
        """Inserts row with new data to table with company results in DB

        :param dataframe: dataframe with results data
        :type dataframe: pd.DataFrame
        :param ticker: stock ticker to use as name of the table
        :type ticker: str
        """ 

        #Rename column that contains the dates to Data
        dataframe.rename(columns={'Unnamed: 0':'Data'}, inplace=True)

        #Sort date values 
        dataframe = dataframe.sort_values(by='Data')
        
        #Get last row i.e the most recent data entry
        dataframe = dataframe.iloc[[-1]]
        
        #Change ticker name to lower case
        ticker = ticker.lower()
        #Getting the connection parameters to the PostgreSQL DB
        params = self.params
        
        #Params to connect to DB
        user = params['user']
        password = params['password']
        port = params['port']
        host = params['host']

        #Changing the database to insert the table
        database = 'fundamentus_results'
        
        #SQL Alchemy engine
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

        #Transform the text in te column to Datetime
        dataframe['Data'] = pd.to_datetime(dataframe['Data'])

        #Asserting the dataframe fits the expected data types
        self.__assert_balance_schema(dataframe)

        #Transforming DataFrame to Table in database
        dataframe.head(0).to_sql(ticker, engine, if_exists='append',index=False)

        #Get connection and cursor to execute the insert
        conn = engine.raw_connection()
        cur = conn.cursor()

        #IO class to read the dataframe in a string and transfer to DB
        output = io.StringIO()
        dataframe.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        
        #Execute the upload
        try: cur.copy_from(output, ticker)     

        #If row already exists, violating the unique constraint, rollback on transaction       
        except UniqueViolation: 
            cur.execute('ROLLBACK')

        #Commit
        conn.commit()

        #Close and finish connections to db
        conn.close()
        engine.dispose()

    def insert_row_balancesheets(self, dataframe:pd.DataFrame, ticker:str):
        """Inserts row with new data to table with company balance sheet in DB

        :param dataframe: dataframe with balance sheet data
        :type dataframe: pd.DataFrame
        :param ticker: stock ticker to use as name of the table
        :type ticker: str
        """ 
        dataframe = dataframe.fillna(0)

        #Rename column that contains the dates to Data
        dataframe.rename(columns={'Unnamed: 0':'Data'}, inplace=True)

        #Sort date values 
        dataframe = dataframe.sort_values(by='Data')
        
        #Get last row i.e the most recent data entry
        dataframe = dataframe.iloc[[-1]]

        #Change ticker name to lower case
        ticker = ticker.lower()

        #Getting the connection parameters to the PostgreSQL DB
        params = self.params
        
        #Params to connect to DB
        user = params['user']
        password = params['password']
        port = params['port']
        host = params['host']

        #Changing the database to insert the table
        database = 'fundamentus_balancesheets'
        
        #SQL Alchemy engine
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

        #Transform the text in te column to Datetime
        dataframe['Data'] = pd.to_datetime(dataframe['Data'])

        #Asserting the dataframe fits the expected data types
        self.__assert_balance_schema(dataframe)

        #Transforming DataFrame to Table in database
        dataframe.head(0).to_sql(ticker, engine, if_exists='append',index=False)

        #Get connection and cursor to execute the insert
        conn = engine.raw_connection()
        cur = conn.cursor()

        #IO class to read the dataframe in a string and transfer to DB
        output = io.StringIO()
        dataframe.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

       
        #Execute the upload
        try: cur.copy_from(output, ticker)     

        #If row already exists, violating the unique constraint, rollback on transaction       
        except UniqueViolation: 
            cur.execute('ROLLBACK')

        #Commit
        conn.commit()

        #Close and finish connections to db
        conn.close()
        engine.dispose()

    def insert_row_events(self, dataframe:pd.DataFrame, ticker:str):
        """Inserts row with new data to table with company events in DB

        :param dataframe: dataframe with events data
        :type dataframe: pd.DataFrame
        :param ticker: stock ticker to use as name of the table
        :type ticker: str
        """ 
        dataframe = dataframe.fillna(0)

        #Change ticker name to lower case
        ticker = ticker.lower()
        #Getting the connection parameters to the PostgreSQL DB
        params = self.params
        
        #Params to connect to DB
        user = params['user']
        password = params['password']
        port = params['port']
        host = params['host']

        #Changing the database to insert the table
        database = 'fundamentus_events'
        
        #SQL Alchemy engine
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
        

        #Transform the text in te column to Datetime
        dataframe['Data'] = pd.to_datetime(dataframe['Data'])

        #Asserting the dataframe fits the expected data types
        self.__assert_events_schema(dataframe)

        #Transforming DataFrame to Table in database
        dataframe.head(0).to_sql(ticker, engine, if_exists='replace',index=False)

        #Get connection and cursor to execute the insert
        conn = engine.raw_connection()
        cur = conn.cursor()

        #IO class to read the dataframe in a string and transfer to DB
        output = io.StringIO()
        dataframe.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        
        #Execute the upload
        cur.copy_from(output, ticker)            
        conn.commit()
        conn.close()
        engine.dispose()

    def insert_row_info(self, dataframe:pd.DataFrame, ticker:str):
        """Inserts table to the general information database

        :param dataframe: Dataframe
        :type dataframe: pd.DataFrame
        :param ticker: stock ticker to use as name of the table
        :type ticker: str

        """     

        #Lower ticker name to lowercase   
        ticker= ticker.lower()

        #Getting the connection parameters to the PostgreSQL DB
        params = self.params

        #Params to connect to DB
        user = params['user']
        password = params['password']
        port = params['port']
        host = params['host']

        #Changing the database to insert the table
        database = 'fundamentus_company_info'

        #SQL Alchemy engine
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

        #Asserting the dataframe fits the expected data types
        self.__assert_info_schema(dataframe)

        #Transforming DataFrame to Table in database
        dataframe.head(0).to_sql(ticker, engine, if_exists='append',index=False)

        #Get connection and cursor to execute the insert
        conn = engine.raw_connection()
        cur = conn.cursor()

        #IO class to read the dataframe in a string and transfer to DB
        output = io.StringIO()
        dataframe.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        
        #Execute the upload
        cur.copy_from(output, ticker)                 

        conn.commit()
        conn.close()
        engine.dispose()

    def __assert_balance_schema(self, df: pd.DataFrame):
        """
        Function to assert data follows data type expectations, specific
        for the balance sheets and company results.

        :param df: Dataframe
        :type df: pd.DataFrame
        """        

        #Asserts if column "Data" exists
        assert 'Data' in df.columns

        #Iterates through columns to assert data type
        for column in df.columns:
            
            #Assert date column is datetime
            if column == 'Data':
                assert df[column].dtypes == 'datetime64[ns]'

            #Assert data column is float or int
            else:                 
                assert np.issubdtype(df[column].dtypes, np.floating) or np.issubdtype(df[column].dtypes, np.integer)

    def __assert_events_schema(self, df:pd.DataFrame):
        """
        Function to assert data follows data type expectations, specific
        for the company events.

        :param df: Dataframe
        :type df: pd.DataFrame
        """   
       
       #Assert presence of columns
        assert all(df.columns == ['Data', 'Descrição'])

        #Assert Date column is datetime
        assert df['Data'].dtypes == 'datetime64[ns]'

        #Assert event description column is String
        assert df['Descrição'].dtypes == 'object'

    def __assert_info_schema(self, df: pd.DataFrame):
        """
        Function to assert data follows data type expectations, specific
        for the stock info.

        :param df: Dataframe
        :type df: pd.DataFrame
        """   
        
        #Columns that should be string
        string_columns = ['Papel','Tipo','Empresa','Setor','Subsetor']

        #Columns that should be Datetime
        date_columns = ['Data últ cot','Últ balanço processado','Data Coletado']

        #Iterate through columns
        for column in df.columns:
            
            #Assert supposed string columns are string
            if column in string_columns:
                assert df[column].dtypes == 'object'
            
            
            elif column in date_columns:
                
                #Assert date column is datetime
                try: assert df[column].dtypes == 'datetime64[ns]'

                #If failed assertion, assert value is Null
                except AssertionError: assert df[column].isnull().values.any()

            else: 
                
                #If not string column nor date column, assert values are float or int
                try: assert np.issubdtype(df[column].dtypes, np.floating) or np.issubdtype(df[column].dtypes, np.integer)

                #If failed assertion, raise assertion error
                except AssertionError: raise AssertionError(f'{column} | {df[column].dtypes} | {df[column].values} | Is NaN: {df[column].values[0] is None}')

    def __query(self, query:str, database:str)->list:
        """Function to run queries in the databases

        :param query: query to be executed
        :type query: str
        :param database: the database in which the query should run
        :type database: str
        :return: query results
        :rtype: list or None for no results
        """        
        
        params = self.params
        params['database']=database

        conn = psycopg2.connect(**params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 
        cur = conn.cursor()
        
        cur.execute(query)
        
       

        try:results = cur.fetchall()
        except psycopg2.ProgrammingError: results = None
        conn.commit()
        conn.close()
        return results
       
    def get_balance_tables(self):
        tables = self.__query("SELECT * FROM information_schema.tables WHERE table_schema = 'public';", 'fundamentus_balancesheets')
        tables = [c[2] for c in tables]
        return tables

    def get_results_tables(self):
        tables = self.__query("SELECT * FROM information_schema.tables WHERE table_schema = 'public';", 'fundamentus_results')
        tables = [c[2] for c in tables]
        return tables

    def get_info_tables(self):
        tables = self.__query("SELECT * FROM information_schema.tables WHERE table_schema = 'public';", 'fundamentus_company_info')
        tables = [c[2] for c in tables]
        return tables

    def get_events_tables(self):
        tables = self.__query("SELECT * FROM information_schema.tables WHERE table_schema = 'public';", 'fundamentus_events')
        tables = [c[2] for c in tables]
        return tables

    
