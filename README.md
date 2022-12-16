# Fundamentus Scrape API

[![Website status](https://img.shields.io/website-up-down-green-red/https/fundamentus-sc-prod-stock-scraper-api-n6ugqu.mo2.mogenius.io.svg?label=Website%20status)](https://fundamentus-sc-prod-stock-scraper-api-n6ugqu.mo2.mogenius.io)

This repository is for extracting data from the brazillian investments web page called Fundamentus. Uses BeautifulSoup and Requests to webscrape the website for data. Data can then be delivered through a Flask API.



[API Usage](#api-usage)

 - [General Stock Information](#general-stock-information-infoticker)
 - [Dividends Data](#stock-dividends-data-dividendsticker)
 - [Company Events](#company-events-eventsticker)
 - [Results and Balance Sheets](#historical-results-and-balance-sheets-fundamentalsticker)


[Code Usage](#code-usage)
 - [General Stock Information](#get_stock_info-ticker-)
 - [Dividends Data](#get_dividends-ticker-)
 - [Company Events](#get_events-ticker-)
 - [Results and Balance Sheets](#get_fundamentals-ticker-)
 - [Async/Bulk Collecting Functions](#bulk-functions)

# 
# API Usage

All endpoints, except for the fundamentals endpoint that returns a zip file, return pandas dataframes delivered as dictionaries. Making easy transformation to dataframes using from_dict().

Requests to the fundamentals endpoint need to encode the request content in bytes, to a zipfile. In python this can be achieved with ZipFile(BytesIO(response.content))
# 
## General Stock Information (/info/**ticker**)

Gets general information of the company, displayed when searching for the ticker on the Fundamentus website.

Example:

```
https://hub-docker-com-prod-stock-scraper-api-n6ugqu.mo2.mogenius.io/info/B3SA3
```
```
{"Ativo":{"1":48771400000},"Ativo Circulante":{"1":18498400000},"Cota\u00e7\u00e3o":{"1":11.74},"Cres. Rec (5a)":{"1":23.9},"Data \u00falt cot":{"1":"Mon, 12 Sep 2022 00:00:00 GMT"},"Disponibilidades":{"1":16748800000},"Div Br/ Patrim":{"1":0.6},"Div. Yield":{"1":5.1},"D\u00edv. Bruta":{"1":12300700000},"D\u00edv. L\u00edquida":{"1":0},"EBIT / Ativo":{"1":11.5},"EV / EBIT":{"1":11.97},"EV / EBITDA":{"1":10.08},"Empresa":{"1":"B3 ON"},"Giro Ativos":{"1":0.2},"LPA":{"1":0.71},"Liquidez Corr":{"1":1.62},"Marg. Bruta":{"1":89.7},"Marg. EBIT":{"1":56.1},"Marg. L\u00edquida":{"1":43.2},"Max 52 sem":{"1":15.88},"Min 52 sem":{"1":9.97},"Nro. A\u00e7\u00f5es":{"1":6099000000},"P/Ativ Circ Liq":{"1":0},"P/Ativos":{"1":1.47},"P/Cap. Giro":{"1":10.07},"P/EBIT":{"1":12.76},"P/L":{"1":16.6},"P/VP":{"1":3.51},"PSR":{"1":7.16},"Papel":{"1":"B3SA3"},"Patrim. L\u00edq":{"1":20417500000},"ROE":{"1":21.1},"ROIC":{"1":17.6},"Setor":{"1":"Servi\u00e7os Financeiros Diversos"},"Subsetor":{"1":"Servi\u00e7os Financeiros Diversos"},"Tipo":{"1":"ON"},"VPA":{"1":3.35},"Valor da firma":{"1":67154200000},"Valor de mercado":{"1":71602300000},"Vol $ m\u00e9d (2m)":{"1":809657000},"\u00dalt balan\u00e7o processado":{"1":"Fri, 30 Sep 2022 00:00:00 GMT"}}
```
# 
## Stock Dividends Data (/dividends/**ticker**)

Gets historical data on company dividends, in case the company has one.

Example:

```
https://hub-docker-com-prod-stock-scraper-api-n6ugqu.mo2.mogenius.io/dividends/B3SA3
```
```
{"Data de Pagamento":{"02/05/2011":"-","04/07/2019":"17/07/2019","09/08/2012":"-","11/06/2014":"-","11/08/2014":"-","11/11/2010":"-","11/11/2013":"-","11/12/2015":"-","12/05/2010":"-","12/11/2009":"-","14/05/2012":"-","14/08/2009":"-","15/04/2015":"-","15/05/2009":"-","17/04/2013":"-","17/11/2011":"-","17/11/2014":"-","18/05/2015":"-","19/05/2014":"-","20/12/2021":"30/12/2021","21/02/2011":"-","21/05/2013":"-","21/06/2011":"-","21/08/2013":"-","21/08/2015":"-","21/08/2017":"-","21/09/2018":"05/10/2018","21/11/2012":"-","21/11/2016":"-","21/11/2017":"-","21/12/2010":"-","21/12/2016":"-","21/12/2017":"-","21/12/2018":"08/01/2019","22/05/2017":"-","22/08/2016":"-","22/09/2022":"07/10/2022","22/12/2009":"-","23/04/2018":"08/05/2018","23/05/2016":"-","23/08/2010":"-","23/11/2015":"-","24/03/2021":"08/04/2021","24/03/2022":"08/04/2022","24/09/2019":"07/10/2019","24/09/2020":"07/10/2020","25/03/2020":"07/04/2020","25/06/2019":"17/07/2019","25/08/2008":"-","26/02/2010":"-","26/03/2019":"05/04/2019","26/03/2021":"08/04/2021","26/08/2011":"-","27/06/2018":"10/07/2018","28/06/2022":"08/07/2022","28/09/2021":"07/10/2021","29/03/2012":"-","29/06/2021":"07/07/2021","30/03/2010":"-","30/04/2009":"-","30/04/2010":"-","30/06/2020":"07/08/2020","30/12/2008":"-","30/12/2019":"30/12/2019","30/12/2020":"08/01/2021","30/12/2021":"07/01/2022"},"Por quantas a\u00e7\u00f5es":{"02/05/2011":1,"04/07/2019":1,"09/08/2012":1,"11/06/2014":1,"11/08/2014":1,"11/11/2010":1,"11/11/2013":1,"11/12/2015":1,"12/05/2010":1,"12/11/2009":1,"14/05/2012":1,"14/08/2009":1,"15/04/2015":1,"15/05/2009":1,"17/04/2013":1,"17/11/2011":1,"17/11/2014":1,"18/05/2015":1,"19/05/2014":1,"20/12/2021":1,"21/02/2011":1,"21/05/2013":1,"21/06/2011":1,"21/08/2013":1,"21/08/2015":1,"21/08/2017":1,"21/09/2018":1,"21/11/2012":1,"21/11/2016":1,"21/11/2017":1,"21/12/2010":1,"21/12/2016":1,"21/12/2017":1,"21/12/2018":1,"22/05/2017":1,"22/08/2016":1,"22/09/2022":1,"22/12/2009":1,"23/04/2018":1,"23/05/2016":1,"23/08/2010":1,"23/11/2015":1,"24/03/2021":1,"24/03/2022":1,"24/09/2019":1,"24/09/2020":1,"25/03/2020":1,"25/06/2019":1,"25/08/2008":1,"26/02/2010":1,"26/03/2019":1,"26/03/2021":1,"26/08/2011":1,"27/06/2018":1,"28/06/2022":1,"28/09/2021":1,"29/03/2012":1,"29/06/2021":1,"30/03/2010":1,"30/04/2009":1,"30/04/2010":1,"30/06/2020":1,"30/12/2008":1,"30/12/2019":1,"30/12/2020":1,"30/12/2021":1},"Tipo":...
```
# 
## Company Events (/events/**ticker**)

Gets important company events displayed on the website. Events like change in directors, results presentation, meetings, etc.

Example

```
https://hub-docker-com-prod-stock-scraper-api-n6ugqu.mo2.mogenius.io/events/B3SA3
```

```
"Descri\u00e7\u00e3o":{"0":"Programa de Recompra de A\u00e7\u00f5es e Equity Swap","1":"Proje\u00e7\u00f5es de despesas, investimentos, alavancagem financeira e distribui\u00e7\u00f5es aos acionistas","2":"Lives da Semana de 21/11/2022","3":"Comunicado ao Mercado - Destaques Operacionais - Outubro 2022","4":"Comunicado ao Mercado - Destaques Operacionais - Outubro 2022","5":"Negocia\u00e7\u00e3o at\u00edpicas de valores mobili\u00e1rios","6":"Aquisi\u00e7\u00e3o da Neurotech","7":"Cronograma de divulga\u00e7\u00e3o de resultados do 3T22","8":"Aquisi\u00e7\u00e3o da Datastock","9":"Aquisi\u00e7\u00e3o da Datastock","10":"Destaques Operacionais - Setembro 2022","11":"Comunicado ao Mercado - Destaques Operacionais - Agosto 2022","12":"Destaques operacionais - Julho/22","13":"Destaques Operacionais - Junho/22","14":"Leil\u00e3o Concess\u00e3o Rodovia ? Exclus\u00e3o do polo passivo de A\u00e7\u00e3o Civil P\u00fablica","15":"Modifica\u00e7\u00e3o de Proje\u00e7\u00f5es Divulgadas","16":"Destaques operacionais - Maio/2022","17":"Comunicado ao Mercado - Destaques Operacionais - Abril 2022","18":"Destaques operacionais - Julho/22","19":"A\u00e7\u00e3o Civil P\u00fablica","20":"Cronograma de divulga\u00e7\u00e3o de resultados do 2T22","21":"Lives da Semana de 01/08/2022","22":"Aprova\u00e7\u00e3o da sexta emiss\u00e3o de Deb\u00eantures simples, n\u00e3o convers\u00edveis em a\u00e7\u00f5es de emiss\u00e3o da B3","23":"Liv...
```
# 
## Historical Results and Balance Sheets (/fundamentals/**ticker**)

Collects historical Balance Sheets and Company Results data files from website's server.
Uses requests Session to collect cookies from website, then uses cookies to validate a GET request to the server and retrieve a Zip file encoded in Bytes

Example

```
https://hub-docker-com-prod-stock-scraper-api-n6ugqu.mo2.mogenius.io/fundamentals/B3SA3

```

To extract the zip file in python you need to use the following code:

```
#Raw response from server in bytes
response = requests.get('.../fundamentals/B3SA3')

#Zip file comes in bytes, we use BytesIO to decode the bytes into zip file
try: z = ZipFile(io.BytesIO(response.content))

#Zip file may be corrupted, always capture this exception
except BadZipFile: ...

```

After that just use **z.extractall()** to get the files in the zip file

**In a browser, the zip file will download automatically.**
# 
# Code Usage

Here is more about how the code works internally with the stock_scraper import
# 
## get_stock_info( ticker )

Gets general information of the company, that is displayed when searching for the ticker on the website.

Example:

```
data = Fundamentus().get_stock_info('B3SA3')
print(data)
```
```
{'Papel': 'B3SA3', 'Tipo': 'ON', 'Empresa': 'B3 ON', 'Setor': 'Serviços Financeiros Diversos', 'Subsetor': 'Serviços Financeiros Diversos', 'Cotação': '12.25', 'Data últ cot': '09/08/2022', 'Min 52 sem': '10.03', 'Max 52 sem': '15.96', 'Vol $ méd (2m)': '398846000', 'Valor de mercado': 74712700000, 'Valor da firma': 69620000000, 'Últ balanço processado': '31/03/2022', 'Nro. Ações': '6099000000', 'P/L': '16.38', 'P/VP': '3.43', 'P/EBIT': '12.42', 
'PSR': '7.34', 'P/Ativos': '1.47', 'P/Cap. Giro': '9.43', 'P/Ativ Circ Liq': '-8.18', 'Div. Yield': '5,8%', 'EV / EBITDA': '9.83', 'EV / EBIT': '11.58', 'Cres. Rec (5a)': '27,6%', 
'LPA': '0.75', 'VPA': '3.58', 'Marg. Bruta': '89,8%', 'Marg. EBIT': '59,1%', 'Marg. Líquida': '44,8%', 'EBIT / Ativo': '11,9%', 'ROIC': '18,8%', 'ROE': '20,9%', 'Liquidez Corr': '1.67', 'Div Br/ Patrim': '0.62', 'Giro Ativos': '0.20', 'Ativo': '50737200000', 'Disponibilidades': '18569100000', 'Ativo Circulante': '19784000000', 'Dív. Bruta': '13476400000', 'Dív. Líquida': '-5092740000', 'Patrim. Líq': '21807200000'}
```
# 
## get_dividends( ticker )

Gets historical data on company dividends, in case the company has one.

Example:

```
data = Fundamentus().get_dividends('B3SA3')
print(data)
```
```
             Valor             Tipo Data de Pagamento  Por quantas ações
Data Com
28/06/2022  0.0609  JRS CAP PROPRIO        08/07/2022                  1
28/06/2022  0.0699        DIVIDENDO        08/07/2022                  1
24/03/2022  0.0502  JRS CAP PROPRIO        08/04/2022                  1
30/12/2021  0.0498  JRS CAP PROPRIO        07/01/2022                  1
...            ...              ...               ...                ...
15/05/2009  0.0559  JRS CAP PRÓPRIO                 -                  1
30/04/2009  0.0303        DIVIDENDO                 -                  1
30/12/2008  0.0692  JRS CAP PRÓPRIO                 -                  1
25/08/2008  0.0730  JRS CAP PRÓPRIO                 -                  1
25/08/2008  0.0700        DIVIDENDO                 -                  1

[82 rows x 4 columns]
```
# 
## get_events( ticker )

Gets important company events displayed on the website. Events like change in directors, results presentation, meetings, etc.

Example

```
data = Fundamentus().get_events('B3SA3')
print(data)
```

```
                                                          Descrição
Data
09/08/2022 18:12                  Destaques operacionais - Julho/22
08/08/2022 22:13                                 Ação Civil Pública
01/08/2022 18:04     Cronograma de divulgação de resultados do 2T22
01/08/2022 09:38                      Lives da Semana de 01/08/2022
25/07/2022 18:52  Aprovação da sexta emissão de Debêntures simpl...
...                                                             ...
24/11/2020 22:44              Destaques Operacionais - Outubro 2020
23/11/2020 09:16                                    Lives da Semana
12/11/2020 18:47                Modificação de Projeções Divulgadas
26/10/2020 08:01                                  Parceria B3 e IRB
19/10/2020 18:24                      Lives da Semana de 19/10/2020

[100 rows x 1 columns]
```
# 
## get_fundamentals( ticker )

Collects Balance Sheets and Company Results historical data files from website's server.
Uses requests Session to collect cookies from website, then uses cookies to make a GET request to the server and retrieve a Zip file encoded in Bytes

Example

```
Fundamentus().get_fundamentals('B3SA3')

```

To extract the zip file you need to use the following code:

```
#Raw response from server in bytes
response = Fundamentus().get_fundamentals('B3SA3')

#Zip file comes in bytes, we use BytesIO to decode the bytes into zip file
try: z = ZipFile(io.BytesIO(response.content))

#Zip file may be corrupted, always capture this exception
except BadZipFile: ...

```
# 
# Bulk Functions

The following functions are similar to the previous ones. But they use *AsyncHTML* for making various asynchronous requests to the website simultaneously. Thus collecting multiple dataframes at the same time, optimizing massive data extraction for multiple tickers/companies
# 
## bulk_get_fundamentals( list_of_tickers, force_download: bool *(Default False)* )

Equal to **get_fundamentals** but with asynchronous requests, made for quick downloading of multiple data files. 

Use *force_download = True* to redownload files already in directory.

Example

```

Fundamentus().bulk_get_fundamentals(['B3SA3','ITSA4','TRPL4'])

```

**NOTE:** This is just an example, you can easily and quickly download 500+ files with this function.
# 
## bulk_get_stock_info(list_of_tickers)

Equal to **get_stock_info** but with asynchronous requests, made for quick downloading of multiple dataframes. Returns a list of tuples as: (dataframe, respective_ticker)


Example

```

Fundamentus().bulk_get_stock_info(['B3SA3','ITSA4','TRPL4'])

```
```

[(pd.DataFrame, 'B3SA3'),(pd.DataFrame, 'ITSA4'),(pd.DataFrame, 'TRPL4')]

```

**NOTE:** This is just an example, you can easily and quickly download 500+ files with this function.
# 
## bulk_get_dividends(list_of_tickers)

Equal to **get_dividends** but with asynchronous requests, made for quick downloading of multiple dataframes. Returns a list of tuples as: (dataframe, respective_ticker)


Example

```

Fundamentus().bulk_get_dividends(['B3SA3','ITSA4','TRPL4'])

```
```

[(pd.DataFrame, 'B3SA3'),(pd.DataFrame, 'ITSA4'),(pd.DataFrame, 'TRPL4')]

```

**NOTE:** This is just an example, you can easily and quickly download 500+ files with this function.
# 
## bulk_get_events(list_of_tickers)

Equal to **get_events** but with asynchronous requests, made for quick downloading of multiple dataframes. Returns a list of tuples as: (dataframe, respective_ticker)


Example

```

Fundamentus().bulk_get_dividends(['B3SA3','ITSA4','TRPL4'])

```

```

[(pd.DataFrame, 'B3SA3'),(pd.DataFrame, 'ITSA4'),(pd.DataFrame, 'TRPL4')]

```

**NOTE:** This is just an example, you can easily and quickly download 500+ files with this function.


