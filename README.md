# Fundamentus Scrape

This repository is for extracting data from the brazillian investments web page called Fundamentus. Uses Selenium and Requests to webscrape
all data on the website

To extract historical fundamentalist data (Profits, Gross Margin, etc) that are protected by captha, we use Convolutional Neural Networks to read the captcha images
and return the text inside it

# Usage

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


## bulk_get_fundamentals( list_of_tickers, force_download: bool *(Default False)* )

Equal to **get_fundamentals** but with asynchronous requests, made for quick downloading of multiple data files. 

Use *force_download = True* to redownload already downloaded files.

Example

```

Fundamentus().bulk_get_fundamentals(['B3SA3','ITSA4','TRPL4'])

```

**NOTE:** This is just an example, you can easily and quickly download 500+ files with this function.

