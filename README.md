# Fundamentus Scrape

Esse repositório serve para extrair dados da página brasileira de investimentos Fundamentus, através das bibliotecas Selenium e Requests. 

Para extrair dados fundamentalistas históricos, que estão protegidos por captcha, usamos uma Rede Neural Convulacional, para ler os captchas e extrair os dados.

# Uso

## get_stock_info( ticker/pregão )

Pega dados gerais da empresa, que são disponibilizados quando se busca um pregão no site.

Exemplo:

```
data = Fundamentus().get_stock_info('B3SA3')
print(data)
```
```
{'Papel': 'B3SA3', 'Tipo': 'ON', 'Empresa': 'B3 ON', 'Setor': 'Serviços Financeiros Diversos', 'Subsetor': 'Serviços Financeiros Diversos', 'Cotação': '12.25', 'Data últ cot': '09/08/2022', 'Min 52 sem': '10.03', 'Max 52 sem': '15.96', 'Vol $ méd (2m)': '398846000', 'Valor de mercado': 74712700000, 'Valor da firma': 69620000000, 'Últ balanço processado': '31/03/2022', 'Nro. Ações': '6099000000', 'P/L': '16.38', 'P/VP': '3.43', 'P/EBIT': '12.42', 
'PSR': '7.34', 'P/Ativos': '1.47', 'P/Cap. Giro': '9.43', 'P/Ativ Circ Liq': '-8.18', 'Div. Yield': '5,8%', 'EV / EBITDA': '9.83', 'EV / EBIT': '11.58', 'Cres. Rec (5a)': '27,6%', 
'LPA': '0.75', 'VPA': '3.58', 'Marg. Bruta': '89,8%', 'Marg. EBIT': '59,1%', 'Marg. Líquida': '44,8%', 'EBIT / Ativo': '11,9%', 'ROIC': '18,8%', 'ROE': '20,9%', 'Liquidez Corr': '1.67', 'Div Br/ Patrim': '0.62', 'Giro Ativos': '0.20', 'Ativo': '50737200000', 'Disponibilidades': '18569100000', 'Ativo Circulante': '19784000000', 'Dív. Bruta': '13476400000', 'Dív. Líquida': '-5092740000', 'Patrim. Líq': '21807200000'}
```

## get_dividends( ticker/pregão )

Pega o histórico de dividendos de uma empresa, caso a empresa tenha um.

Exemplo:

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

## get_events( ticker/pregão )

Pega eventos importantes da empresa, disponibilizados no site fundamentos. Eventos como divulgação de resultados, mudanças na diretoria, reuniões, etc.

Exemplo

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

## get_fundamentals( lista de pregões )

Essa é a parte mais complexa do código, irá iterar pelos pregões e irá extrair dados fundamentalistas históricos das empresas. Pra isso é necessário usar o [NoCaptcha](https://github.com/Iuryck/CNN-Captcha-Reader) para ler e resolver os captchas que "protegem" os dados automaticamente, essa é a parte mais demorada do processo. Depois de extraídos os dados serão organizados dentro da pasta Fundamentus criado no diretório.

Exemplo

```
Fundamentus().get_fundamentals(['B3SA3', 'TRPL4','ITSA4'])

```