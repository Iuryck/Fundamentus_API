
from selenium import webdriver

import os
import time
import pandas as pd
import requests
import shutil
from tqdm import tqdm

import zipfile
from zipfile import ZipFile


import pandas as pd
import datetime as dt
import os
import pandas as pd
import xlrd
from webdriver_manager.chrome import ChromeDriverManager
import re
import traceback
from cv2 import error as cv2_error






class Fundamentus():

    def treat_exception(self, ticker, exception):
        f = open(f'ERROR_LOG.txt', 'w')
        f.write(ticker+'\n')
        f.write(str(exception))
        f.write('\n_________________________________________________________\n')
        f.write(traceback.format_exc())
        f.close()

    def get_tickers(self):
        """Retorna um dataframe com empresas e códigos de ações listados no site Fundamentos

        :return: Dataframe com ações e empresas
        :rtype: pd.DataFrame
        """     
        #Header definindo como o requests irá simular um usuário acessando o navegador
        header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
        }

        #Lendo a página do Fundamentos que possui as empresas e ações
        
        r = requests.get('https://www.fundamentus.com.br/detalhes.php', headers=header)            

        if r.status_code == 404:
            raise ConnectionError('Failed to connect to website, check internet connection or if https://www.fundamentus.com.br/detalhes.php is a valid website')

        #Passando o código html para o Pandas, que vai ler como uma lista de dataframes
        dfs = pd.read_html(r.text)
        
        #Lendo o primeiro dataframe da lista que possui as empresas e ações
        df = pd.DataFrame(dfs[0])
        
        #Retornando        
        return df

    def __separate_data(self,tickers):
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
            df.to_csv(os.path.abspath('Fundamentus')+'\\Data_Categories\\'+column+'.csv')


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
            df.to_csv(os.path.abspath('Fundamentus')+'\\Data_Categories\\'+column+'.csv')

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

    def get_events(self,ticker):
        """Pega eventos e acontecimentos de uma empresa listada no site Fundamentus

        :param ticker: pregão da empresa que deseja pegar os eventos
        :type ticker: string
        :return: dataframe de eventos de uma empresa
        :rtype: pandas.DataFrame
        """        
        
        #Url do site onde estão os eventos da empresa do pregão
        url = f"https://www.fundamentus.com.br/fatos_relevantes.php?papel={ticker}"

        #Configurando o agente pelo qual o Requests irá operar
        header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
        }
        
        #Pegando a resposta em html
        r = requests.get(url, headers=header)
        
        #Lendo o html com Pandas, transformando os dados em html em uma lista de dataframes
        dfs = pd.read_html(r.text,decimal=',', thousands='.')
        
        #Pegando os dados que importam
        df = dfs[0][['Data', 'Descrição']] 

        #Configurando as datas dos eventos para serem índices do dataframe
        df.index = df['Data']

        #Retirando a coluna de datas, já que esses dados estão no índice
        df = df.drop('Data', axis=1)

        return df

    def get_stock_info(self,ticker):
        """Pega informações gerais sobre uma empresa no site do Fundamentus através de requests.

        :param ticker: pregão da empresa para pegar os dados
        :type ticker: string
        :return: dicionário com as informações
        :rtype: dict
        """        

        url = f"https://www.fundamentus.com.br/detalhes.php?papel={ticker}"

    
        header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
        }
    
        r = requests.get(url, headers=header)
        company_df = pd.DataFrame()
        dfs = pd.read_html(r.text,decimal=',', thousands='.')
        
        
        company_df = dfs[0].loc[:, 0:1].append(dfs[0].loc[:, 2:3].rename(columns={2:0,3:1}))
        company_df.index = company_df.loc[:,0]
        company_df.drop(0, axis=1, inplace=True)

        df = dfs[1].loc[:, 0:1].append(dfs[1].loc[:, 2:3].rename(columns={2:0,3:1}))
        df.index = df.loc[:,0]
        df.drop(0, axis=1, inplace=True)
        company_df = company_df.append(df)

        
        df = dfs[2].loc[1:, 2:3].rename(columns={2:0,3:1}).append(dfs[2].loc[1:, 4:5].rename(columns={4:0,5:1}))
        df.index = df.loc[:,0]
        df.drop(0, axis=1, inplace=True)
        company_df = company_df.append(df)

        df = dfs[3].loc[1:, 0:1].append(dfs[3].loc[1:, 2:3].rename(columns={2:0,3:1}))
        df.index = df.loc[:,0]
        df.drop(0, axis=1, inplace=True)
        company_df = company_df.append(df)

        

        temp_dict = company_df.to_dict()[1]
        dictionary = {}
        for c in temp_dict.keys(): 
            dictionary[str(c).replace('?','')] = temp_dict[c]
            

        return dictionary

    def get_dividends(self, ticker):
        """Pega informações sobre os dividendos de uma empresa através de requests, se ela tiver dividendos

        :param tickers: lista de pregões
        :type tickers: list
        :raises ConnectionError: erro de conexão caso a página retorne erro 404
        """        

        

        #A url que você quer acesssar
        url = f"https://www.fundamentus.com.br/proventos.php?papel={ticker}"

        #Informações para fingir ser um navegador
        header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
        }
        #Juntamos tudo com a requests
        r = requests.get(url, headers=header)
        
        if r.status_code == 404:
            raise ConnectionError('Failed to connect to website, check internet connection or if https://www.fundamentus.com.br/detalhes.php is a valid website')

        #E finalmente usamos a função read_html do pandas
        try: dfs = pd.read_html(r.text, decimal=',', thousands='.')
        except ValueError: pass

        #Pega o DataFrame que queremos  
        df = dfs[0]
        
        #Renomeia a data como Data Com, explicitando a informação
        df.rename({'Data':'Data Com'}, axis=1,inplace=True)

        #Colocando as datas como índice
        df = df.set_index('Data Com', drop=True)
        return df
                
    def get_fundamentals(self,tickers):
        from NoCaptcha.NoCaptcha import NoCaptchaGPU as NC

        if not os.path.exists(os.path.abspath('Fundamentus')):
            os.mkdir(os.path.abspath('Fundamentus'))

        if not os.path.exists(os.path.abspath('Fundamentus')+'\Original'):
            os.mkdir(os.path.abspath('Fundamentus')+'\Original')

        

        options = webdriver.ChromeOptions()
        prefs = {"download.default_directory" : os.path.abspath('Fundamentus')+'\Original'}
        options.add_experimental_option("prefs",prefs)

        driver = webdriver.Chrome(ChromeDriverManager().install() , options=options)
        
        success_list = [c[:c.find('.')] for c in os.listdir('Fundamentus\\Original') if '.zip' in c]

        nc = NC()
        nc.build()

        for ticker in tqdm(tickers):
            
            if ticker in success_list: continue

            def get_fund(ticker):
                captcha=''
                driver.get(f"https://fundamentus.com.br/balancos.php?papel={ticker}&tipo=1")

                
                with open('Fundamentus\\filename.png', 'wb') as file:
                    file.write(driver.find_element_by_xpath("/html/body/div[1]/div[2]/form/img").screenshot_as_png)
                

                time.sleep(2)
                captcha_input = driver.find_element_by_id("codigo_captcha")
                try: 
                    
                    
                    captcha, paths = nc.crack_captcha(os.path.abspath('Fundamentus')+'\\filename.png', save_data=True)
                    

                    captcha_input.send_keys(captcha)
                    
                except RuntimeError:
                    captcha = 'failed'
                    captcha_input.send_keys(captcha)

                except cv2_error:
                    captcha = 'failed'
                    captcha_input.send_keys(captcha)
                
                
                time.sleep(0.2)

                driver.find_element_by_xpath("/html/body/div[1]/div[2]/form/input[4]").click()
                time.sleep(1)
                captcha_input = driver.find_element_by_id("codigo_captcha")
                
                if captcha_input.get_attribute('value') == captcha:
                    filename = max([os.path.abspath('Fundamentus')+'\Original' + "\\" + f for f in os.listdir(os.path.abspath('Fundamentus\\Original'))],key=os.path.getctime)

                    letters=[c for c in captcha]

                    for i, letter in enumerate(letters):
                        shutil.copy(paths[i], f'NoCaptcha\\Letters\\{letter}')
                        

                    
                    try: os.rename(filename, os.path.abspath('Fundamentus')+f'\Original\\{ticker}.zip')
                    except FileExistsError as e:
                        
                        os.remove(os.path.abspath('Fundamentus')+f'\Original\\{ticker}.zip')
                        os.rename(filename, os.path.abspath('Fundamentus')+f'\Original\\{ticker}.zip')              

                    
                else: get_fund(ticker)
                
            try: get_fund(ticker)
            except Exception as e: self.treat_exception(ticker, e)
            os.remove('Fundamentus\\filename.png')

        driver.close()

        print('____________________ UNZIPING FILES ____________________')
        self.__unzip_fundamentals(tickers)
        print('___________________ SEPARATING SHEETS __________________')
        self.__separate_data(tickers)
        print('___________________ RENAMING COLUMNS __________________')
        self.__rename_columns()
        print('___________________ SEPARATING COLUMNS __________________')
        self.__separate_category(tickers)

if __name__ == '__main__':

    ticker = 'B3SA3'
    
    '''tickers = list(Fundamentus().get_tickers()['Papel'])
    tickers_letters = [c[:4] for c in tickers]
    ticker_dict = dict(zip(tickers_letters,tickers))
    new_tickers = list(ticker_dict.values())'''
    data = Fundamentus().get_events(ticker)
    print(data)

   



