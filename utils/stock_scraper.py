import io
import os
import pandas as pd
import requests
from requests import Response
from tqdm import tqdm
import zipfile
from zipfile import BadZipFile, ZipFile
import pandas as pd
import datetime as dt
import os
import pandas as pd
import xlrd
import re
import traceback
from requests_html import AsyncHTMLSession
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
warnings.filterwarnings("ignore")
from utils.update_db import Update_Fundamentus





class Fundamentus():

    def treat_exception(self, exception):
        f = open(f'FUNDAMENTUS_ERROR_LOG.txt', 'w')
        f.write(str(exception)+'\n')
        f.write(str(exception.__class__)+'\n')
        f.write('\n_________________________________________________________\n')
        f.write(traceback.format_exc())
        f.close()

    def get_tickers(self)->pd.DataFrame:
        """Returns a dataframe with companies and their tickers that are listed on the Fundamentus website

        :return: Dataframe with tickers and companies
        :rtype: pd.DataFrame
        """     
        #Header defining the simulated browser to make requests
        header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
        }

        #Reading webpage that has the tickers and companies available in the website
        r = requests.get('https://www.fundamentus.com.br/detalhes.php', headers=header)            

        #Raise for request error 404
        if r.status_code == 404:
            raise ConnectionError('Failed to connect to website, check internet connection or if https://www.fundamentus.com.br/detalhes.php is a valid website')

        #Passing html code to pandas to read the dataframes from the html code, returns a list of dataframes
        dfs = pd.read_html(r.text)
        
        #Passing first dataframe which is the dataframe we need
        df = pd.DataFrame(dfs[0])
            
        return df

    def get_events(self,ticker:str)->pd.DataFrame:
        """
        Gets company events from a company listed in the Fundamentus website
       

        :param ticker: Company ticker to get events from
        :type ticker: str
        :return: Dataframe with company events, listed with date and description of event
        :rtype: pandas.DataFrame
        """        
        
        #Url where company events of the ticker are stored
        url = f"https://www.fundamentus.com.br/fatos_relevantes.php?papel={ticker}"

        #Header defining the simulated browser to make requests
        header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
        }
        
        #Reading webpage
        r = requests.get(url, headers=header)
        
        #Reading the html code into dataframes with pandas, returns list of dataframes
        dfs = pd.read_html(r.text,decimal=',', thousands='.')
        
        #Getting the dataframe that actually has the data we want
        df = dfs[0][['Data', 'Descrição']] 

        return df

    def get_stock_info(self,ticker:str)->pd.DataFrame:
        """Gets general information on a company ticker, such as company sector, profit margin, last stock price, etc.

        :param ticker: Company ticker to get data
        :type ticker: str
        :return: Dataframe with the general info
        :rtype: pd.DataFrame
        """        

        #Url where company general info is stored
        url = f"https://www.fundamentus.com.br/detalhes.php?papel={ticker}"

        #Header defining the simulated browser to make requests
        header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
        }

        #Reading webpage 
        r = requests.get(url, headers=header)

        #Empty dataframe to store the company info
        company_df = pd.DataFrame()

        #Reading HTML code to pandas dataframes, returns list of dataframes
        dfs = pd.read_html(r.text,decimal=',', thousands='.')
        
        
        '''
        The web page that has the company general information has various small tables with pieces of information in different
        forms and shapes. The following code is to format and concat all the small tables until we have a proper traditional dataframe 
        with all the data we want
        '''

        #Formatting first dataframe and passing to main dataframe
        company_df = pd.concat([dfs[0].loc[:, 0:1],dfs[0].loc[:, 2:3].rename(columns={2:0,3:1})])
        company_df.index = company_df.loc[:,0]
        company_df.drop(0, axis=1, inplace=True)

        #Formatting second dataframe and concatenating to main dataframe
        if len(dfs)>1:
            df = pd.concat([dfs[1].loc[:, 0:1], dfs[1].loc[:, 2:3].rename(columns={2:0,3:1})])
            df.index = df.loc[:,0]
            df.drop(0, axis=1, inplace=True)
            company_df = pd.concat([company_df, df]) 

        #Formatting third dataframe and concatenating to main dataframe
        if len(dfs)>2:
            df = pd.concat([dfs[2].loc[1:, 2:3].rename(columns={2:0,3:1}), dfs[2].loc[1:, 4:5].rename(columns={4:0,5:1})])
            df.index = df.loc[:,0]
            df.drop(0, axis=1, inplace=True)
            company_df = pd.concat([company_df, df]) 

        #Formatting fourth dataframe and concatenating to main dataframe
        if len(dfs)>3:
            df = pd.concat([dfs[3].loc[1:, 0:1], dfs[3].loc[1:, 2:3].rename(columns={2:0,3:1})]) 
            df.index = df.loc[:,0]
            df.drop(0, axis=1, inplace=True)
            company_df = pd.concat([company_df, df]) 
        
        #Removing unwanted chars in index, which has column names
        company_df.index = [str(c).replace('?','') for c in company_df.index]

        #Transposing to make index into columns
        company_df = company_df.transpose()   

        #List of columns that are percentages that we will format as float
        pct_columns = ['Div. Yield','Marg. Bruta','Marg. Líquida','Marg. EBIT','EBIT / Ativo','ROIC','ROE','Cres. Rec (5a)','P/Ativ Circ Liq']

        #List of columns that should be datetime and will be formatted as such
        date_columns = ['Data últ cot','Últ balanço processado']

        #List of columns that should be string and will be formatted as such
        string_columns = ['Papel','Tipo','Empresa','Setor','Subsetor']

        '''
        These dataframes with company info are very very unstructured
        and unpredictable, following code tries its best to format data in a
        correct way and make the data more predictable and structured
        '''

        for column in company_df.columns:
            
            #Sometimes a Null value is indicated with a minus sign
            if any(['-' in str(c) for c in company_df[column]]):
                
                #Replace with None as string in string columns
                if column in string_columns:
                    company_df[column] = 'None'
                
                #Replace with a basic None in date columns
                elif column in date_columns:
                    company_df[column] = None
                
                #Replace with a 0 in numeric columns
                else:
                    company_df[column] = 0

                #Skip column after data formatted
                continue
            
            #If column with percentage
            if column in pct_columns:

                #Get the text value in column
                text = company_df[column].iloc[0]

                #Checks if there is more than one comma in text
                if len([c for c in text if c==','])>1:
                    
                    #Number of commas that don't indicate decimal value
                    commas = len([c for c in text if c==','])-1  

                    #Remove commas that don't indicate decimals
                    text = text.replace(',','',commas)
                    
                    #Remvoe percent sign and replace comma with dot to make float format
                    text = text.replace('%','').replace(',','.')
                
                #If just one comma, than just change text for float format
                else:
                    text = text.replace(',','.').replace('%','')

                #Sometimes dots are used instead of commas to indicate thousands
                if len([c for c in text if c=='.'])>1:

                    #Number of dots that don't indicate decimal value
                    commas = len([c for c in text if c=='.'])-1

                    #Remove unwanted dots and percent sign
                    text = text.replace('.','',commas).replace('%','')

                #Replace old string with new float number
                company_df[column] = text
                company_df[column] = float(company_df[column])  

                #Skip column after formatting is done
                continue    
            
            #If date column, make datetime data type
            if column in date_columns:          
                company_df[column] = pd.to_datetime(company_df[column])  

                #Skip column after formatting is done
                continue
            

            #If none of the above, we try to find a fit data type
            try: 
                
                #Try to convert to float
                company_df[column] = company_df[column].astype(float)     

                #If all values are int, convert
                if company_df[column].apply(float.is_integer).all():   
                    company_df[column] = company_df[column].astype(int)         

            #If it is not possible to convert to int or float, try string
            except ValueError: 
                company_df[column] = company_df[column].astype(str)
            
            #If any null values in column
            if company_df[column].isnull().values.any():
                
                #String None for string columns
                if column in string_columns:
                    company_df[column] = 'None'

                #Date columns leave with none, assume non str or date are numeric
                elif column not in date_columns:

                    #0 for numeric columns
                    company_df[column] = 0

                  
        

        return company_df

    def get_dividends(self, ticker:str)->pd.DataFrame:
        """Pega informações sobre os dividendos de uma empresa através de requests, se ela tiver dividendos

        :param ticker: 
        :type ticker: str
        :raises ConnectionError: erro de conexão caso a página retorne erro 404
        """        

        

        #URL with the ticker dividends data
        url = f"https://www.fundamentus.com.br/proventos.php?papel={ticker}"

        #Header defining the simulated browser to make requests
        header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
        }
        
        #Reading webpage that has the ticker's dividends data
        r = requests.get(url, headers=header)
        
        if r.status_code == 404:
            raise ConnectionError('Failed to connect to website, check internet connection or if https://www.fundamentus.com.br/detalhes.php is a valid website')

        #Reading HTML code to pandas dataframes, returns list of dataframes
        try: dfs = pd.read_html(r.text, decimal=',', thousands='.')
        except ValueError: pass

        #Gets the dataframe with the data we need
        df = dfs[0]
        
        #Renaming the date column to "Data Com" to explicit that the column is the buy-in date for the dividends
        df.rename({'Data':'Data Com'}, axis=1,inplace=True)

        #Setting dates as index
        df = df.set_index('Data Com', drop=True)

        return df

    def get_fundamentals(self, ticker:str)->bytes:
        """Gets the zip file, in bytes, for fundamentalist data on ticker company through requests. 
        To parse and extract the zip file use: ZipFile( io.BytesIO(get_fundamentals(ticker)) )

        :param ticker: ticker of company to extract fundamentalist data
        :type ticker: str
        :return: Zip file with data in Bytes
        :rtype: bytes
        """        

        #URL that gives access to the data zip file
        url = f'https://fundamentus.com.br/balancos.php?papel={ticker}&tipo=1'

        #Request headers
        header = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            
            }

        #Request session to collect cookies
        with requests.Session() as s:

            #Get ticker website with data zip file with async request
            r = s.get(url, headers=header)

            #Create referer header to get the right zip file
            header.update({"Referer": f'{url}'})

            #Gets cookie with session ID to be able to make request which retrieves zip file in bytes
            cookie = {'PHPSESSID': requests.utils.dict_from_cookiejar(s.cookies)['PHPSESSID']}

            #Request to collect ticker data zip file
            r = s.get(f'https://fundamentus.com.br/planilhas.php?SID={cookie["PHPSESSID"]}', headers=header)
            
        return r.content

    def bulk_get_fundamentals(self,tickers:list[str], force_download:bool=False):
        """Function to get various Zip files with fundamentalist data from various companies quickly and efficiently. Uses
        AsyncHTML to do requests to various pages simultaneously, then iterates and extracts the Zip files for each ticker.

        :param tickers: list of tickers to extract fundamentalist data from
        :type tickers: list[str]
        :param force_download: Download a Zip file even if zip file is already in default folder, defaults to False
        :type force_download: bool, optional
        """        
        
      

        #Create folders to store the data
        if not os.path.exists(os.path.abspath('Fundamentus')):
            os.mkdir(os.path.abspath('Fundamentus'))

        if not os.path.exists(os.path.abspath('Fundamentus')+'\Original'):
            os.mkdir(os.path.abspath('Fundamentus')+'\Original')

        
        #List of archives already downloaded to skip in the loop. If force_download is true, creates empty list
        if not force_download: success_list = [c[:c.find('.')] for c in os.listdir('Fundamentus\\Original') if '.xls' in c]
        else : success_list=[]

        
        #Creates list of tuples with the URL to make the requests and the ticker to keep track of each ticker and their file
        urls = [(f'https://fundamentus.com.br/balancos.php?papel={ticker}&tipo=1', ticker) for ticker in tickers if ticker not in success_list]
       
        #Creating iterable of callables to the function that retrieves the zip file in bytes, passing each url and ticker from urls list
        tasks = (lambda url=url, ticker=ticker: self.__get_zip_request(url, ticker) for url, ticker in urls)
        
        #Unpacks the list of callables and runs all of them simultaneously
        results = AsyncHTMLSession().run(*tasks)

        #Iterating through tickers and their zip files
        for response, ticker in tqdm(results):   
            

            #Zip file comes in bytes, we use BytesIO to decode the bytes into zip file
            try: z = ZipFile(io.BytesIO(response.content))

            #If zip file is corrupted, skip
            except BadZipFile: continue

            #Extract zip content
            z.extractall("Fundamentus\\Original")

            #Gets the newly collected zip file in directory
            filename = max([os.path.abspath('Fundamentus')+'\Original' + "\\" + f for f in os.listdir(os.path.abspath('Fundamentus\\Original'))],key=os.path.getctime)

            #Tries to rename the newly collected file to the ticker name
            try: os.rename(filename, os.path.abspath('Fundamentus')+f'\Original\\{ticker}.xls')

            #If the ticker file name already exists, deletes the file, renames again
            except FileExistsError:
                
                os.remove(os.path.abspath('Fundamentus')+f'\Original\\{ticker}.xls')
                os.rename(filename, os.path.abspath('Fundamentus')+f'\Original\\{ticker}.xls')              
            
        
        print('___________________ SEPARATING SHEETS __________________')
        self.__separate_data()
        print('___________________ RENAMING COLUMNS __________________')
        self.__rename_columns()

    def bulk_get_dividends(self, tickers:list[str])->list[tuple[pd.DataFrame, str]]:
        """Function to get various dataframes with dividends data from various companies quickly and efficiently. Uses
        AsyncHTML to do requests to various pages simultaneously, then iterates and extracts the dataframes for each ticker.

        :param tickers: list of tickers to extract data from
        :type tickers: list[str]
        :return: list of tickers and their respective dataframes
        :rtype: list[tuple[pd.DataFrame, str]]
        """  
        
        #Creates list of tuples with the URL to make the requests and the ticker to keep track of each ticker and their dataframe
        urls = [(f'https://www.fundamentus.com.br/proventos.php?papel={ticker}', ticker) for ticker in tickers]
       
        #Creating iterable of callables to the function that retrieves the dividends HTML page, passing each url and ticker from urls list
        tasks = (lambda url=url, ticker=ticker: self.__get_async_request(url, ticker) for url, ticker in urls)
        
        #Unpacks the list of callables and runs all of them simultaneously
        results = AsyncHTMLSession().run(*tasks)

        #List for dataframes and their respective ticker
        dataframes = []

        #Iterating through the HTML pages and their respective ticker
        for r, ticker in results:
            
            #Reading in the HTML code to pandas dataframe
            try: dfs = pd.read_html(r.text, decimal=',', thousands='.')

            #Tickers that don't have dividends won't have any tables, thus raising ValueError from pandas
            except ValueError: continue

            #Gets the dataframe with the data we need
            df = dfs[0]
            
            #Renaming the date column to "Data Com" to explicit that the column is the buy-in date for the dividends
            df.rename({'Data':'Data Com'}, axis=1,inplace=True)

            #Setting dates as index
            df = df.set_index('Data Com', drop=True)

            #Appending dataframe and ticker to list
            dataframes.append((df, ticker))

        return dataframes

    def bulk_get_stock_info(self, tickers:list[str]):
        """Function to get various dataframes with general info from various companies quickly and efficiently. Uses
        AsyncHTML to do requests to various pages simultaneously, then iterates and extracts the dataframes for each ticker.

        :param tickers: list of tickers to extract data from
        :type tickers: list[str]
        :return: list of tickers and their respective dataframes
        :rtype: list[tuple[pd.DataFrame, str]]
        """  
        
        #Creates list of tuples with the URL to make the requests and the ticker to keep track of each ticker and their dataframe
        urls = [(f'https://www.fundamentus.com.br/detalhes.php?papel={ticker}', ticker) for ticker in tickers]
       
        #Creating iterable of callables to the function that retrieves the dividends HTML page, passing each url and ticker from urls list
        tasks = (lambda url=url, ticker=ticker: self.__get_async_request(url, ticker) for url, ticker in urls)
        
        #Unpacks the list of callables and runs all of them simultaneously
        results = AsyncHTMLSession().run(*tasks)

        #List for dataframes and their respective ticker
        dataframes = []

        #Iterating through the HTML pages and their respective ticker
        for r, ticker in results:
            
            #Empty dataframe to store the company info
            company_df = pd.DataFrame()

            #Reading HTML code to pandas dataframes, returns list of dataframes
            dfs = pd.read_html(r.text,decimal=',', thousands='.')


            '''
            The web page that has the company general information has various small tables with pieces of information in different
            forms and shapes. The following code is to format and concat all the small tables until we have a proper traditional dataframe 
            with all the data we want
            '''

            #Formatting first dataframe and passing to main dataframe
            company_df = pd.concat([dfs[0].loc[:, 0:1],dfs[0].loc[:, 2:3].rename(columns={2:0,3:1})])
            company_df.index = company_df.loc[:,0]
            company_df.drop(0, axis=1, inplace=True)

            #Formatting second dataframe and concatenating to main dataframe
            if len(dfs)>1:
                df = pd.concat([dfs[1].loc[:, 0:1], dfs[1].loc[:, 2:3].rename(columns={2:0,3:1})])
                df.index = df.loc[:,0]
                df.drop(0, axis=1, inplace=True)
                company_df = pd.concat([company_df, df]) 

            #Formatting third dataframe and concatenating to main dataframe
            if len(dfs)>2:
                df = pd.concat([dfs[2].loc[1:, 2:3].rename(columns={2:0,3:1}), dfs[2].loc[1:, 4:5].rename(columns={4:0,5:1})])
                df.index = df.loc[:,0]
                df.drop(0, axis=1, inplace=True)
                company_df = pd.concat([company_df, df]) 

            #Formatting fourth dataframe and concatenating to main dataframe
            if len(dfs)>3:
                df = pd.concat([dfs[3].loc[1:, 0:1], dfs[3].loc[1:, 2:3].rename(columns={2:0,3:1})]) 
                df.index = df.loc[:,0]
                df.drop(0, axis=1, inplace=True)
                company_df = pd.concat([company_df, df]) 
            
            #Removing unwanted chars in index, which has column names
            company_df.index = [str(c).replace('?','') for c in company_df.index]

            #Transposing to make index into columns
            company_df = company_df.transpose()   

            #List of columns that are percentages that we will format as float
            pct_columns = ['Div. Yield','Marg. Bruta','Marg. Líquida','Marg. EBIT','EBIT / Ativo','ROIC','ROE','Cres. Rec (5a)','P/Ativ Circ Liq']

            #List of columns that should be datetime and will be formatted as such
            date_columns = ['Data últ cot','Últ balanço processado']

            #List of columns that should be string and will be formatted as such
            string_columns = ['Papel','Tipo','Empresa','Setor','Subsetor']

            '''
            These dataframes with company info are very very unstructured
            and unpredictable, following code tries its best to format data in a
            correct way and make the data more predictable and structured
            '''

            for column in company_df.columns:
                
                #Sometimes a Null value is indicated with a minus sign
                if any(['-' in str(c) for c in company_df[column]]):
                    
                    #Replace with None as string in string columns
                    if column in string_columns:
                        company_df[column] = 'None'
                    
                    #Replace with a basic None in date columns
                    elif column in date_columns:
                        company_df[column] = None
                    
                    #Replace with a 0 in numeric columns
                    else:
                        company_df[column] = 0

                    #Skip column after data formatted
                    continue
                
                #If column with percentage
                if column in pct_columns:

                    #Get the text value in column
                    text = company_df[column].iloc[0]

                    #Checks if there is more than one comma in text
                    if len([c for c in text if c==','])>1:
                        
                        #Number of commas that don't indicate decimal value
                        commas = len([c for c in text if c==','])-1  

                        #Remove commas that don't indicate decimals
                        text = text.replace(',','',commas)
                        
                        #Remvoe percent sign and replace comma with dot to make float format
                        text = text.replace('%','').replace(',','.')
                    
                    #If just one comma, than just change text for float format
                    else:
                        text = text.replace(',','.').replace('%','')

                    #Sometimes dots are used instead of commas to indicate thousands
                    if len([c for c in text if c=='.'])>1:

                        #Number of dots that don't indicate decimal value
                        commas = len([c for c in text if c=='.'])-1

                        #Remove unwanted dots and percent sign
                        text = text.replace('.','',commas).replace('%','')

                    #Replace old string with new float number
                    company_df[column] = text
                    company_df[column] = float(company_df[column])  

                    #Skip column after formatting is done
                    continue    
                
                #If date column, make datetime data type
                if column in date_columns:          
                    company_df[column] = pd.to_datetime(company_df[column])  

                    #Skip column after formatting is done
                    continue
                

                #If none of the above, we try to find a fit data type
                try: 
                    
                    #Try to convert to float
                    company_df[column] = company_df[column].astype(float)     

                    #If all values are int, convert
                    if company_df[column].apply(float.is_integer).all():   
                        company_df[column] = company_df[column].astype(int)         

                #If it is not possible to convert to int or float, try string
                except ValueError: 
                    company_df[column] = company_df[column].astype(str)
                
                #If any null values in column
                if company_df[column].isnull().values.any():
                    
                    #String None for string columns
                    if column in string_columns:
                        company_df[column] = 'None'

                    #Date columns leave with none, assume non str or date are numeric
                    elif column not in date_columns:

                        #0 for numeric columns
                        company_df[column] = 0
                    

                    
                           

                                   
            #Create column for the time in which these informations were collected
            company_df['Data Coletado'] = dt.date.today()

            #Transform to pandas Datetime
            company_df['Data Coletado'] = pd.to_datetime(company_df['Data Coletado'])

            dataframes.append((company_df, ticker))

        return dataframes

    def bulk_get_events(self, tickers:list[str]):
        """Function to get various dataframes with company events from various companies quickly and efficiently. Uses
        AsyncHTML to do requests to various pages simultaneously, then iterates and extracts the dataframes for each ticker.

        :param tickers: list of tickers to extract data from
        :type tickers: list[str]
        :return: list of tickers and their respective dataframes
        :rtype: list[tuple[pd.DataFrame, str]]
        """  
        
        #Creates list of tuples with the URL to make the requests and the ticker to keep track of each ticker and their dataframe
        urls = [(f'https://www.fundamentus.com.br/fatos_relevantes.php?papel={ticker}', ticker) for ticker in tickers]
       
        #Creating iterable of callables to the function that retrieves the events HTML page, passing each url and ticker from urls list
        tasks = (lambda url=url, ticker=ticker: self.__get_async_request(url, ticker) for url, ticker in urls)
        
        #Unpacks the list of callables and runs all of them simultaneously
        results = AsyncHTMLSession().run(*tasks)

        #List for dataframes and their respective ticker
        dataframes = []

        #Iterating through the HTML pages and their respective ticker
        for r, ticker in results:

            #Reading HTML with pandas to retrieve list of dataframes in webpage
            try: 
                dfs = pd.read_html(r.text,decimal=',', thousands='.')
                
                #Getting the dataframe we want
                df = dfs[0][['Data', 'Descrição']] 

                #Remove unwanted typos in string
                df['Descrição'] = df['Descrição'].str.replace(r'\t',' ')
            
            #Raise value error if no tables are found
            except ValueError as e:
                
                #Creates empty dataframe if no events found
                if str(e) == 'No tables found':
                    df=pd.DataFrame(columns=['Data','Descrição'])
                
                #Raise exception if different value error
                else: raise e            
            
            #Append dataframe and respective ticker
            dataframes.append((df, ticker))

        return dataframes

    async def __get_zip_request(self,url:str, ticker:str)->tuple[Response, str]:
        """Makes async request to retrieve the fundamentalist data zip file.
        Different from get_async_request because a session and cookies are needed
        to retrieve data

        :param url: URL to the ticker's data website
        :type url: str
        :param ticker: ticker to retrieve data for
        :type ticker: str
        :return: tuple with request response and respective ticker
        :rtype: tuple[Response, str]
        """        

        #Request headers
        header = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            
            }

        #Request session to collect cookies
        with AsyncHTMLSession() as s:

            #Get ticker website that has the data zip file with async request
            r = await s.get(url, headers=header)

            #Create referer header to get the right zip file for the right ticker
            header.update({"Referer": f'{url}'})

            #Gets cookie with session ID to be able to make request which retrieves zip file in bytes
            cookie = {'PHPSESSID': requests.utils.dict_from_cookiejar(s.cookies)['PHPSESSID']}

            #Request to collect ticker data zip file
            r = await s.get(f'https://fundamentus.com.br/planilhas.php?SID={cookie["PHPSESSID"]}', headers=header)

        return r, ticker# also returns ticker to keep track of which zip file is from which ticker

    async def __get_async_request(self,url:str, ticker:str)->tuple[Response, str]:
        """Makes async request to retrieve HTML page of url

        :param url: URL of webpage to collect data for ticker
        :type url: str
        :param ticker: ticker to retrieve data for
        :type ticker: str
        :return: tuple with request response and ticker
        :rtype: tuple[Response, str]
        """        

        #Request headers
        header = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            
            }

        #Request session to collect cookies
        with AsyncHTMLSession() as s:

            #Get ticker website that contains data with async request
            r = await s.get(url, headers=header)

        return r, ticker# also returns ticker to keep track of which data is from which ticker

    def __separate_data(self):
        """Recebe uma lista de ações e separa, para cada ação, as 2 planilhas que vem nos arquivos dos balanços,
        uma planilha de balanços e outra planilha de demonstrativos. Também transforma as duas planilhas de forma
        que fique mais fácil de usar e ler os dados.

        :param tickers: Lista de código de ações (Ex: ABEV3)
        :type tickers: list
        """        
        
        #Criando diretório para salvar as planilhas de balanço, caso não exista
        if not os.path.exists('Fundamentus\\Balanco'):
            os.mkdir('Fundamentus\\Balanco')

        #Criando diretório para salvar as planilhas de Resultados Demonstrativos, caso não exista.
        if not os.path.exists('Fundamentus\\Resultados_Demonstrativos'):
                os.mkdir('Fundamentus\\Resultados_Demonstrativos')

        success_list = [c[:c.find('.')] for c in os.listdir('Fundamentus\\Original') if '.xls' in c]

        #Iterando pelas ações
        for ticker in tqdm(success_list):
            
            #Abrindo o arquivo com a bibliotexa xlrd para arquivos Excel
            wb = xlrd.open_workbook(f'Fundamentus\\Original\\{ticker}.xls', logfile=open(os.devnull, 'w'))
            

            #Passando o arquivo excel para o Pandas
            df = pd.ExcelFile(wb, engine='xlrd')
            
            #Separando a planilha de balanços
            balance = df.parse(df.sheet_names[0])

            #Separando a planilha de resultados demonstrativos
            results = df.parse(df.sheet_names[1])

            #______________________________ Tratando Balanços ______________________________#

            '''
            Nessa planilha, o nome dos dados vem como índice e as datas em que os dados foram formalizados vem como colunas.
            O que queremos aqui é mudar esse formato para seguir o padrão de dados comum para dados em série de tempo. Com nome
            de variáveis nome de colunas, e datas como índice da planilha. Vamos fazer uma transposição da planilha e retirar também
            algumas linhas com informações indesejadas, como o nome da empresa que também vem no topo da planilha
            '''

            #Salvando as datas da planilha que são usadas como colunas para usar como índice na planilha final
            index = balance.loc[0, :].dropna().values

            #Salvando as variáveis da planilha que são usados como índice para usarmos como coluna na planilha final
            columns = balance.transpose().iloc[0 , :].dropna().values

            #Virando a planilha para que podemos usar as datas como índice e variáveis como colunas, sem linhas desnecessárias -> [0,0]
            balance = balance.transpose().iloc[1:,1:]

            #Garantindo que o índice seja salvo como uma variável de Datetime, visto que são datas
            balance.index = [dt.datetime.strptime(c, '%d/%m/%Y') for c in index]

            #Ordenando índice por data
            balance.sort_index(inplace=True)

            #Colocando o nome das colunas que salvamos antes
            balance.columns = columns

            
            
            
            balance.to_csv(f'Fundamentus\\Balanco\\{ticker}.csv')

            #______________________________ Tratando Demonstrativos ______________________________#

            '''
            Aqui a planilha vem parecida com a planilha dos balanços, e teremos que fazer a mesma coisa mas de maneira diferente.
            '''

            #Trocando os valores das colunas por números ordenados, para podermos acessar e manipular as colunas
            results.columns = range(results.shape[1])

            #Pegando os nomes das variáveis da tabela e colocando como índice, para depois da transposição virarem colunas
            results.index = results.loc[:,0]
            
            #Dropando a coluna 0 que tem o nome das variáveis, por ser uma cópia do índice.
            results = results.drop(0, axis=1)
            
            #Passando as datas como nome de coluna, para virarem índice depois da transposição
            results.columns = results.loc[results.index[0],:]

            #Dropando as primeiras linhas que possuem as datas, sendo uma cópia do nome das colunas
            results = results.drop(results.index[0], axis=0)
            
            #Transpondo os dados para que os índices virem colunas e vice versa
            results = results.transpose()   
            
            '''
            Aqui pegamos o índice de data e transformamos num objeto DateTime. Se o índice tiver um dado
            nulo nós o removemos todas as linhas com o dado nulo e continuamos a operação de transformar o índice para
            DateTime.
            '''

            #Pegando o índice
            index=results.index

            #Criando lista vazia para o novo índice
            new_index = []

            #Iterando pelo índice
            for  i in index:
                
                '''
                Caso não conseguimos transformar o índice em DateTime, verificamos se é um dado nulo e removemos as linhas
                que possuem esse índice nulo, e seguimos com a transformação do índice.
                '''

                #Tenta transformar o índice para string e depois para DateTime
                try:
                    new_index.append( dt.datetime.strptime(str(i), '%d/%m/%Y'))     
              
                
                except Exception as e: 
                    
                    #Verifica se há indíce nulo e remove as linhas caso haja
                    if 'nan' in str(e):
                        results = results[results.index.notna()]

                    #Caso não seja esse o erro, levantamos ele
                    else: raise e

                    pass


            
            #Passando o novo índice para a tabela
            results.index = new_index

            #Ordenamos a tabela de acordo com a data
            results = results.sort_index()
            
            #Salvamos a tabela
            results.to_csv(f'Fundamentus\\Resultados_Demonstrativos\\{ticker}.csv')

    def __unzip_fundamentals(self,tickers):
        """Remove os arquivos XSL com os dados financeiros de dentro dos arquivos Zip extraídos do site

        :param tickers: lista de pregões com arquivos Zip para extrair dados
        :type tickers: list
        
        """        


        

        for ticker in tqdm(tickers):
            try:
                with ZipFile(f'Fundamentus\\Original\\{ticker}.zip', 'r') as zip_ref:
                    zip_ref.extractall('Fundamentus\\Original')

                filename = max([os.path.abspath('Fundamentus')+'\Original' + "\\" + f for f in os.listdir(os.path.abspath('Fundamentus')+'\Original')],key=os.path.getctime)
                
                try: os.rename(filename, os.path.abspath('Fundamentus')+f'\Original\\{ticker}.xls')
                except FileExistsError as e:
                    
                    os.remove(os.path.abspath('Fundamentus')+f'\Original\\{ticker}.xls')
                    os.rename(filename, os.path.abspath('Fundamentus')+f'\Original\\{ticker}.xls')
                    
                
                os.remove(os.path.abspath('Fundamentus')+f'\Original\\{ticker}.zip')

                


            except zipfile.BadZipFile as e:
                pass

            except FileNotFoundError: pass
        
    def __separate_category(self,tickers):
        """Abre os arquivos de dados sobre balanço patrimonial e resultado financeiro. E para cada classe de dados (ativos, ganhos com vendas, despesas), pega dados dessa classe de
        todas as empresas e junta em um arquivo. Criando assim um conjunto de arquivos onde dados de todas as empresas podem ser vistas lado a lado. Os pregões e a data dos dados constroem
        a tabela de dados e a classe de dados dá o nome do arquivo.

        :param tickers: pregões das empresas cuja os dados serão transformados
        :type tickers: list
        """        
        


        
        #Lista de colunas para pegar todas as colunas
        columns =[]

        #Variavel para o conjunto de dados que possui indice maior
        index_reference = ''

        #Variavel para comparar indices
        higher_index = -99999

        #Diretório dos dados
        dir = os.path.abspath('Fundamentus')+'\\Balanco\\'

        '''Pegando o arquivo que tem o indice mais longo, usaremos o indice dele para o novo dataframe'''

        #Iterando pelos arquivos de dados
        for i, data in enumerate(os.listdir(os.path.abspath('Fundamentus')+'\\Balanco')):
            
            #Lendo os dados do arquivo
            df = pd.read_csv(dir+data)

            #Juntando todas as diferentes colunas, usaremos essa lista para juntar os dados das empresas por coluna
            [columns.append(c) for c in df.columns]

            #Encontrando o maior indice
            if len(df.index) > higher_index:
                higher_index = len(df.index)
                index_reference = data

        #Lendo o arquivo de dados que tem o maior indice
        index_reference = pd.read_csv(dir+index_reference, index_col=0).index.copy()

        #Removendo colunas repitidas da nossa lista
        columns = list(set(columns))
        
        #Iterando por todas as colunas que existem em meio aos arquivos
        for column in tqdm(columns[1:]):
            
            #Criando novo dataframe com o indice maior, onde vamos juntar os dados
            df = pd.DataFrame(index=index_reference)

            
            #Iterando pelos arquivos de dados denovo
            for i, ticker in enumerate(os.listdir(os.path.abspath('Fundamentus')+'\\Balanco')):

                #Lendo o arquivo de dados
                ticker_df = pd.read_csv(dir+ticker, index_col=0)

                #Pegando o pregão da empresa através do nome do arquivo
                ticker = ticker[:ticker.find('.')]

                
                if column not in ticker_df.columns: continue
                #Pegando a coluna de dados que queremos do arquivo, colocando em uma coluna nomeada pelo pregão
                ticker_df[ticker] = ticker_df[column]

                #Juntando a coluna que queremos ao nosso novo dataframe, indexado por data
                df = df.merge(ticker_df[ticker], left_on=df.index, right_on=ticker_df.index, how='left').set_index('key_0', drop=True)
                
               
            

            #Criando pasta para alocar nosso novo dataframe, com as colunas das empresas agrupadas
            if not os.path.exists(os.path.abspath('Fundamentus')+'\\Data_Categories'):
                os.mkdir(os.path.abspath('Fundamentus')+'\\Data_Categories')

            #Salvando nosso novo dataframe
            df.to_csv(os.path.abspath('Fundamentus')+f'\\Data_Categories\\{column}.csv')


        '''Agora repetimos o mesmo processo para os Resultados Demonstrativos'''


        #Diretório dos dados
        dir = os.path.abspath('Fundamentus')+'\\Resultados_Demonstrativos\\'

        #Lista de colunas para pegar todas as colunas
        columns =[]

        #Variavel para o conjunto de dados que possui indice maior
        index_reference = ''

        #Variavel para comparar indices
        higher_index = -99999

        #Iterando pelos arquivos de dados
        for data in os.listdir(os.path.abspath('Fundamentus')+'\Resultados_Demonstrativos'):
            #Lendo os dados do arquivo
            df = pd.read_csv(dir+data)

            #Juntando todas as diferentes colunas, usaremos essa lista para juntar os dados das empresas por coluna
            [columns.append(c) for c in df.columns]

            #Encontrando o maior indice
            if len(df.index) > higher_index:
                higher_index = len(df.index)
                index_reference = data
        
        #Lendo o arquivo de dados que tem o maior indice
        index_reference = pd.read_csv(dir+index_reference, index_col=0).index.copy()
        
        #Removendo colunas repitidas da nossa lista
        columns = list(set(columns))
        
        #Iterando por todas as colunas que existem em meio aos arquivos
        for column in tqdm(columns[1:]):
            
            #Criando novo dataframe com o indice maior, onde vamos juntar os dados
            df = pd.DataFrame(index=index_reference)

            
            #Iterando pelos arquivos de dados denovo
            for i, ticker in enumerate(os.listdir(os.path.abspath('Fundamentus')+'\Resultados_Demonstrativos')):
                
                #Lendo o arquivo de dados
                ticker_df = pd.read_csv(dir+ticker, index_col=0)

                #Pegando o pregão da empresa através do nome do arquivo
                ticker = ticker[:ticker.find('.')]

                try:
                    #Pegando a coluna de dados que queremos do arquivo, colocando em uma coluna nomeada pelo pregão
                    ticker_df[ticker] = ticker_df[column]

                    #Juntando a coluna que queremos ao nosso novo dataframe, indexado por data
                    df = df.merge(ticker_df[ticker], left_on=df.index, right_on=ticker_df.index, how='left').set_index('key_0', drop=True)

                #Caso a coluna não existe nos dados, segue para o próximo
                except KeyError as e: 
                    pass
            
           

            #Criando pasta para alocar nosso novo dataframe, com as colunas das empresas agrupadas
            if not os.path.exists(os.path.abspath('Fundamentus')+'\\Data_Categories'):
                os.mkdir(os.path.abspath('Fundamentus')+'\\Data_Categories')

            #Salvando nosso novo dataframe
            df.to_csv(os.path.abspath('Fundamentus')+f'\\Data_Categories\\{column}.csv')

    def __rename_columns(self):
        """
        Função para renomear as colunas dos Balanços das empresas, tendo em vista que os arquivos originais formatam os dados
        de formas nada convencionais. Isso irá ajudar a entender e extrair os dados de uma maneira melhor.
        Motivo de não iterarmos pelos Resultados Demonstrativos é que esses arquivos não ficam com colunas repitidas quando fazemos as tranformações necessárias neles.

        """        



        #Dicionário com os nomes atuais como chaves, e os nomes que queremos como valores
        rename_columns = {

            'Estoques':'Estoques_AtivoCirculante',
            'Estoques.1' : 'Estoques_AtivoRealizávelALongoPrazo',
            'Provisões': 'Provisões_PassivoCirculante',
            'Provisões.1': 'Provisões_PassivoNãoCirculante',
            'Adiantamento para Futuro Aumento Capital':'AdiantamentoParaFuturoAumentoCapital_PassivoNãoCirculante',
            'Adiantamento para Futuro Aumento Capital.1':'Adiantamento para Futuro Aumento Capital_Patrimônio_Líquido',
            'Despesas Antecipadas':'DespesasAntecipadas_AtivoCirculante',
            'Despesas Antecipadas.1':'DespesasAntecipadas_AtivoRealizávelALongoPrazo',
            'Contas a Receber':'ContasAReceber_AtivoCirculante',
            'Contas a Receber.1':'ContasAReceber_AtivoRealizávelALongoPrazo',
            'Empréstimos e Financiamentos':'EmpréstimosEFinanciamentos_PassivoCirculante',
            'Empréstimos e Financiamentos.1':'EmpréstimosEFinanciamentos_PassivoNãoCirculante',
            'Tributos Diferidos':'TributosDiferidos_AtivoRealizávelALongoPrazo',
            'Tributos Diferidos.1':'TributosDiferidos_PassivoNãoCirculante',
            'Passivos com Partes Relacionadas':'PassivosComPartesRelacionadas_PassivoCirculante',
            'Passivos com Partes Relacionadas.1':'PassivosComPartesRelacionadas_PassivoNãoCirculante',
            'Outros':'Outros_PassivoCirculante',
            'Outros.1':'Outros_PassivoNãoCirculante',
            'Ativos Biológicos':'AtivosBiológicos_AtivoCirculante',
            'Ativos Biológicos.1':'AtivosBiológicos_AtivoRealizávelALongoPrazo',
            'Aplicações Interfinanceiras de Liquidez':'AplicaçõesInterfinanceirasDeLiquidez_AtivoCirculante',
            'Aplicações Interfinanceiras de Liquidez.1':'AplicaçõesInterfinanceirasDeLiquidez_AtivoRealizávelALongoPrazo',
            'Títulos e Valores Mobiliários':'TítulosEValoresMobiliários_AtivoCirculante',
            'Títulos e Valores Mobiliários.1':'TítulosEValoresMobiliários_AtivoRealizávelALongoPrazo',
            'Outras Obrigações':'OutrasObrigações_PassivoCirculante',
            'Outras Obrigações.1':'OutrasObrigações_PassivoExigivelALongoPrazo',
            'Relações Interdependências':'RelaçõesInterdependências_AtivoCirculante',
            'Relações Interdependências.1': 'RelaçõesInterdependências_AtivoRealizávelALongoPrazo',
            'Relações Interdependências.2':'RelaçõesInterdependências_PassivoCirculante',
            'Relações Interdependências.3':'RelaçõesInterdependências_PassivoExigivelALongoPrazo',
            'Relações Interfinanceiras':'RelaçõesInterfinanceiras_AtivoCirculante',
            'Relações Interfinanceiras.1':'RelaçõesInterfinanceiras_AtivoRealizávelALongoPrazo',
            'Relações Interfinanceiras.2':'RelaçõesInterfinanceiras_PassivoCirculante',
            'Relações Interfinanceiras.3':'RelaçõesInterfinanceiras_PassivoExigivelALongoPrazo',
            'Passivos sobre Ativos Não_Correntes a Venda e Descontinuados':'PassivosSobreAtivosNãoCorrentesAVendaEDescontinuados_PassivoCirculante',
            'Passivos sobre Ativos Não_Correntes a Venda e Descontinuados.1':'PassivosSobreAtivosNãoCorrentesAVendaEDescontinuados_PassivoNãoCirculante',
            'Operações de Crédito':'OperaçõesDeCrédito_AtivoCirculante',
            'Operações de Crédito.1':'OperaçõesDeCrédito_AtivoRealizávelALongoPrazo',
            'Captações no Mercado Aberto':'CaptaçõesNoMercadoAberto_PassivoCirculante',
            'Captações no Mercado Aberto.1':'CaptaçõesNoMercadoAberto_PassivoExigivelALongoPrazo',
            'Depósitos':'Depósitos_PassivoCirculante',
            'Depósitos.1':'Depósitos_PassivoExigivelALongoPrazo',
            'Obrigações por Empréstimos':'ObrigaçõesPorEmpréstimos_PassivoCirculante',
            'Obrigações por Empréstimos.1':'ObrigaçõesPorEmpréstimos_PassivoExigivelALongoPrazo',
            'Obrigações por Repasse do Exterior':'ObrigaçõesPorRepasseDoExterior_PassivoCirculante',
            'Obrigações por Repasse do Exterior.1':'ObrigaçõesPorRepasseDoExterior_PassivoExigivelALongoPrazo',
            'Operações de Arrendamento Mercantil':'OperaçõesDeArrendamentoMercantil_AtivoCirculante',
            'Operações de Arrendamento Mercantil.1':'OperaçõesDeArrendamentoMercantil_AtivoRealizávelALongoPrazo',
            'Obrigações por Repasse do País':'ObrigaçõesPorRepasseDoPaís_PassivoCirculante',
            'Obrigações por Repasse do País.1':'ObrigaçõesPorRepasseDoPaís_PassivoExigivelALongoPrazo',
            'Recursos de Aceites e Emissão de Títulos':'RecursosDeAceitesEEmissãoDeTítulos_PassivoCirculante',
            'Recursos de Aceites e Emissão de Títulos.1':'RecursosDeAceitesEEmissãoDeTítulos_PassivoExigivelALongoPrazo',
            'Outros Créditos':'OutrosCréditos_AtivoCirculante',
            'Outros Créditos.1':'OutrosCréditos_AtivoRealizávelALongoPrazo',
            'Outros Valores e Bens':'OutrosValoresEBens_AtivoCirculante',
            'Outros Valores e Bens.1':'OutrosValoresEBens_AtivoRealizávelALongoPrazo'      


        }

        #Listando arquivos
        files = os.listdir('Fundamentus\Balanco')

        #Iterando pelos arquivos
        for file in tqdm(files):
            df = pd.read_csv(f'Fundamentus\Balanco\{file}', index_col=0)

            #Lista para armazenar novos nomes para as colunas, depois de armazenar todos, vamos substituir as colunas atuais com elas
            new_columns = []

            #Iterando pelas colunas do dataframe
            for column in df.columns:
                if column=='Unnamed':continue
                
                #Pegando o novo nome para a coluna no dicionário, usando o nome atual como chave
                if column in rename_columns.keys():
                    column = rename_columns.get(column) 

                #Formatando o nome
                column = re.sub(r'\W+', '', column)

                #Adicionando a lista
                new_columns.append(column)
            
            #Substituindo as colunas
            df.columns = new_columns

            #Salvando o dataframe com nome das colunas alteradas
            df.to_csv(f'Fundamentus\Balanco\{file}')


        rename_columns ={
            'Receita Bruta de Vendas e/ou Serviços': 'ReceitaBrutaDeVendasServiços',
            'Deduções da Receita Bruta': 'DeduçõesReceitaBruta',
            'Receita Líquida de Vendas e/ou Serviços': 'ReceitaLíquidaDeVendasServiços',
            'Custo de Bens e/ou Serviços Vendidos': 'CustoDeBensServiçosVendidos',
            'Resultado Bruto': 'ResultadoBruto',
            'Despesas Com Vendas': 'DespesasComVendas',
            'Despesas Gerais e Administrativas': 'DespesasGeraisEAdministrativas',
            'Perdas pela Não Recuperabilidade de Ativos': 'PerdasPelaNãoRecuperabilidadeAtivos',
            'Outras Receitas Operacionais':'OutrasReceitasOperacionais',
            'Outras Despesas Operacionais': 'OutrasDespesasOperacionais',
            'Resultado da Equivalência Patrimonial': 'ResultadoDaEquivalênciaPatrimonial',
            'Financeiras':'Financeiras',
            'Receitas Financeiras': 'ReceitasFinanceiras',
            'Despesas Financeiras':'DespesasFinanceiras',
            'Resultado Não Operacional': 'ResultadoNãoOperacional',
            'Receitas':'Receitas',
            'Despesas':'Despesas',
            'Resultado Antes Tributação/Participações': 'ResultadoAntesTributaçãoParticipações',
            'Provisão para IR e Contribuição Social':'ProvisãoParaIRContribuiçãoSocial',
            'IR Diferido':'IRDiferido',
            'Participações/Contribuições Estatutárias':'ContribuiçõesEstatutárias',
            'Reversão dos Juros sobre Capital Próprio': 'ReversãoJurosSobreCapitalPróprio',
            'Part. de Acionistas Não Controladores': 'Part.AcionistasNãoControladores',
            'Lucro/Prejuízo do Período':'LucroPrejuízoDoPeríodo'

            
        }

        #Listando arquivos
        files = os.listdir('Fundamentus\Resultados_Demonstrativos')

        #Iterando pelos arquivos
        for file in tqdm(files):
            df = pd.read_csv(f'Fundamentus\Resultados_Demonstrativos\{file}', index_col=0)

            #Lista para armazenar novos nomes para as colunas, depois de armazenar todos, vamos substituir as colunas atuais com elas
            new_columns = []

            #Iterando pelas colunas do dataframe
            for column in df.columns:

                if column=='Unnamed':continue
                
                #Pegando o novo nome para a coluna no dicionário, usando o nome atual como chave
                if column in rename_columns.keys():
                    column = rename_columns.get(column) 

                #Formatando o nome
                column = re.sub(r'\W+', '', column)

                #Adicionando a lista
                new_columns.append(column)
            
            #Substituindo as colunas
            df.columns = new_columns

            #Salvando o dataframe com nome das colunas alteradas
            df.to_csv(f'Fundamentus\Resultados_Demonstrativos\{file}')

if __name__ == '__main__':

    fundamentos = Fundamentus()

    update = Update_Fundamentus()

    #df = pd.read_csv('Fundamentus\Resultados_Demonstrativos\AALR3.csv')
    #print(df.fillna(0))


    
    tickers = [c for c in fundamentos.get_tickers()['Papel']]
 
    events = fundamentos.bulk_get_events(tickers)

    update.update_events(events)

    infos = fundamentos.bulk_get_stock_info(tickers)

    update.update_stock_info(infos)

    fundamentos.bulk_get_fundamentals(tickers)

    update.update_balance_sheets()
    update.update_results_sheets()


    
