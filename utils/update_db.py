import numpy as np
from utils.db_connect import Fundamentus_DB
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import os
from psycopg2.errors import *




class Update_Fundamentus:

    def __init__(self) -> None:
        self.db = Fundamentus_DB()
        

    def create_upload_threads(self, fn: object, list:list)->list:
        """
        Creates the thread arguments for a given upload task,
        arguments are used to pass to Thread Pool

        :param fn: function to run as a thread task
        :type fn: object
        :param list: list of args for the function
        :type list: list
        :return: List of thread arguments for the given function/task
        :rtype: list[dict]
        """   

        #Transforms list into np.array   
        arays_split = np.array(list)

        #Splits the array in single pieces for the thread pool
        arays_split = np.array_split(arays_split, len(list))
        
        #List to append the thread args that will be returned
        threads = []
        
        #Iterating through each piece of the split array and giving that slice to a thread
        for aray in arays_split:
            
            #Creating and appending the thread args
            thread = {'fn': fn, 'args':aray.tolist()[0]}
            threads.append(thread)

        return threads

    def update_events(self,data:list[tuple[pd.DataFrame,str]])->None:   
        """
        Function to bulk upload company events tables, inserting new
        tables where there are none, updating data where there is

        :param data: List of tuples with the dataframes and there respective ticker
        :type data: list[tuple[pd.DataFrame,str]]
        """        
        
        #Gets current tables in the database, is returns None if no tables exist
        events_tables = self.db.get_events_tables()


        if events_tables is not None: 
            
            #Gets the tables/dataframes that do not exist in DB for insertion
            new_tables = [c for c in data if c[1].lower() not in events_tables]

            #Gets the tables/dataframes that do exist in DB for updating
            update_tables = [c for c in data if c[1].lower() in events_tables]

        #List for thread futures
        futures = []

        if new_tables:
            
            #Creates threads to upload tables with the new_events_table function
            threads = self.create_upload_threads(self.db.new_events_table, new_tables)

            with ThreadPoolExecutor(max_workers=100) as executor:
                
                #Runs all upload tasks
                for thread in threads:
                    
                    future = executor.submit(thread.get('fn'), thread.get('args')[0], thread.get('args')[1])
                    
                    #Appends futures to list
                    futures.append(future)

            #Wait for futures results to check for errors
            [c.result() for c in futures]

        #Empty futures list
        futures = []

        if update_tables:
            
            #Creates threads to update tables with the insert_row_events function
            threads = self.create_upload_threads(self.db.insert_row_events, update_tables)

            with ThreadPoolExecutor(max_workers=100) as executor:
                
                #Runs all update tasks
                for thread in threads:
                    
                    future = executor.submit(thread.get('fn'), thread.get('args')[0], thread.get('args')[1])

                    #Appends futures to list
                    futures.append(future)

            #Wait for futures results to check for errors
            [c.result() for c in futures]

    def update_stock_info(self,data:list[tuple[pd.DataFrame,str]])->None:
        """
        Function to bulk upload stock info tables, inserting new
        tables where there are none, updating data where there is

        :param data: List of tuples with the dataframes and there respective ticker
        :type data: list[tuple[pd.DataFrame,str]]
        """        
        #Get list of current tables/tickers in DB, returns None if no tables
        info_tables = self.db.get_info_tables()

        if info_tables is not None: 
            
            #Gets the tables/dataframes that do not exist in DB for insertion
            new_tables = [c for c in data if c[1].lower() not in info_tables]

            #Gets the tables/dataframes that do exist in DB for updating
            update_tables = [c for c in data if c[1].lower() in info_tables]

        #List to append thread futures
        futures = []

        if new_tables:
            
            #Creates threads to upload tables with the new_info_table function
            threads = self.create_upload_threads(self.db.new_info_table, new_tables)

            with ThreadPoolExecutor(max_workers=100) as executor:
                
                #Runs all upload tasks
                for thread in threads:
                    
                    future = executor.submit(thread.get('fn'), thread.get('args')[0], thread.get('args')[1])
                    
                    #Appends thread futures to list
                    futures.append(future)

            #Wait for futures results to check for errors
            [c.result() for c in futures]

        #List to append thread futures
        futures = []

        if update_tables:
            
            #Creates threads to update tables with the insert_row_info function
            threads = self.create_upload_threads(self.db.insert_row_info, update_tables)

            with ThreadPoolExecutor(max_workers=100) as executor:
                
                #Runs all upload tasks
                for thread in threads:
                    
                    future = executor.submit(thread.get('fn'), thread.get('args')[0], thread.get('args')[1])
                    
                    #Appends thread futures to list
                    futures.append(future)
            
            #Wait for futures results to check for errors
            [c.result() for c in futures]

    def update_balance_sheets(self)->None:
        """
        Function to bulk upload company balance sheets tables, inserting new
        tables where there are none, updating data where there is.

        Since this data must be extracted from zip and xls files, the function already
        lists and collects the dataframes from local folders and files
        """

        #List balance sheet files in default directory
        balance_sheets = os.listdir('Fundamentus\\Balanco')

        #Get list of current tables/tickers in DB, returns None if no tables
        tables = self.db.get_balance_tables()
        

        if tables is not None: 
            
            #Gets the tables/dataframes that do exist in DB for updating
            update_tables =  [c for c in balance_sheets if c[:-4].lower() in tables]

            #Gets the tables/dataframes that do not exist in DB for insertion
            balance_sheets = [c for c in balance_sheets if c[:-4].lower() not in tables]

        #Format string path to csv file
        balance_sheets = ['Fundamentus\\Balanco\\'+c for c in balance_sheets]

        #Read in all dataframes from the csv file paths, add in tuple with company ticker 
        balance_sheets = [(pd.read_csv(c), c.split('\\')[-1][:-4]) for c in balance_sheets]          

        #List to append thread futures
        futures = []

        if balance_sheets:
            
            #Create upload threads with new_balancesheets_table function
            threads = self.create_upload_threads(self.db.new_balancesheets_table, balance_sheets)

            with ThreadPoolExecutor(max_workers=100) as executor:
                
                #Runs all upload tasks
                for thread in threads:
                    
                    future = executor.submit(thread.get('fn'), thread.get('args')[0], thread.get('args')[1])

                    #Append future
                    futures.append(future)
            
            #Wait for thread results to check for errors
            [c.result() for c in futures]

        #Empty list
        futures = []
        
        if update_tables:

            #Format string path for csv files
            balance_sheets = ['Fundamentus\\Balanco\\'+c for c in update_tables]

            #Read in all dataframes that will update DB, add in tuple with company ticker
            balance_sheets = [(pd.read_csv(c), c.split('\\')[-1][:-4]) for c in balance_sheets]

            #Create updating threads with insert_row_results function
            threads = self.create_upload_threads(self.db.insert_row_balancesheets, balance_sheets)

            with ThreadPoolExecutor(max_workers=100) as executor:
                
                #Runs all upload tasks
                for thread in threads:
                    
                    future = executor.submit(thread.get('fn'), thread.get('args')[0], thread.get('args')[1])

                    #Append thread futures
                    futures.append(future)
            
            #Wait for thread results to check for errors
            [c.result() for c in futures]

    def update_results_sheets(self)->None:
        """
        Function to bulk upload company results tables, inserting new
        tables where there are none, updating data where there is.

        Since this data must be extracted from zip and xls files, the function already
        lists and collects the dataframes from local folders and files
        """

        #List results files in default directory
        results_sheets = os.listdir('Fundamentus\\Resultados_Demonstrativos')

        #Get list of current tables/tickers in DB, returns None if no tables
        tables = self.db.get_results_tables()
        

        if tables is not None: 
            
            #Gets the tables/dataframes that do exist in DB for updating
            update_tables =  [c for c in results_sheets if c[:-4].lower() in tables]

            #Gets the tables/dataframes that do not exist in DB for insertion
            results_sheets = [c for c in results_sheets if c[:-4].lower() not in tables]

        #Format string path to csv file
        results_sheets = ['Fundamentus\\Resultados_Demonstrativos\\'+c for c in results_sheets]

        #Read in all dataframes from the csv file paths, add in tuple with company ticker 
        results_sheets = [(pd.read_csv(c), c.split('\\')[-1][:-4]) for c in results_sheets]          

        #List to append thread futures
        futures = []

        if results_sheets:
            
            #Create upload threads with new_results_table function
            threads = self.create_upload_threads(self.db.new_results_table, results_sheets)

            with ThreadPoolExecutor(max_workers=100) as executor:
                
                #Runs all upload tasks
                for thread in threads:
                    
                    future = executor.submit(thread.get('fn'), thread.get('args')[0], thread.get('args')[1])
                    
                    #Append future
                    futures.append(future)

            #Wait for thread results to check for errors
            [c.result() for c in futures]    

        #Empty list 
        futures = []

        
        if update_tables:
            
            #Format string path for csv files
            results_sheets = ['Fundamentus\\Resultados_Demonstrativos\\'+c for c in update_tables]

            #Read in all dataframes that will update DB, add in tuple with company ticker
            results_sheets = [(pd.read_csv(c), c.split('\\')[-1][:-4]) for c in results_sheets]

            #Create updating threads with insert_row_results function
            threads = self.create_upload_threads(self.db.insert_row_results, results_sheets)

            with ThreadPoolExecutor(max_workers=100) as executor:
                
                #Runs all update tasks
                for thread in threads:
                    
                    future = executor.submit(thread.get('fn'), thread.get('args')[0], thread.get('args')[1])
                    
                    #Append thread futures
                    futures.append(future)

            #Wait for thread results to check for errors
            [c.result() for c in futures]
        
       
       
        
        
        
    






