"""
Este script realiza operações em dados de servidores públicos obtidos do portal da transparência do estado de São Paulo.
O objetivo geral é extrair o rendimento total de um servidor de um determinado órgão público. Como a maioria dos sites de 
transparencia fornecem dados no formato CSV, a opção 3.1 pode ser replicada para outros ambientes além do https://www.transparencia.sp.gov.br (Governo de SP).
A estratégia utilizada para arquivos CSV consiste em:
    1 - Identificar o arquivo CSV mensal de remunerações mais recente; [obter_links_csv_mais_recentes]
    2 - Extrair todos os registros de remuneração transformando-os em um dataframe; [ler_csv_e_transformar_em_servidores]
    3 - Buscar na lista de servidores por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [self.filter_by_email_login(email)]   
  

Autor: https://github.com/stgustavo
Data: 2024-03-09

"""

from bs4 import BeautifulSoup
import re
from io import StringIO
import pandas as pd
from commons.AbstractETL import AbstractETL 
from datetime import datetime
from unidecode import unidecode

# Constantes
URL_PORTAL_TRANSPARENCIA = "https://www.transparencia.sp.gov.br"
PATH_PORTAL_REMUNERACOES = "/PortalTransparencia-Report/Remuneracao.aspx"
PATH_PORTAL_CSV = "/PortalTransparencia-Report/txt/RemuneracaoAtivos.csv"

class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores do Estado de São Paulo.
    
    Domínio implementado: 
    - sp.gov.br   

    """   
    def __init__(self):
        super().__init__(dominio="sp.gov.br",
                         unidade_federativa="São Paulo",
                         portal_remuneracoes_url=URL_PORTAL_TRANSPARENCIA+PATH_PORTAL_REMUNERACOES, 
                         fn_obter_link_mais_recente=self.obter_links_csv_mais_recentes,
                         fn_ler_fonte_de_dados_e_transformar_em_dataframe=self.ler_csv_e_transformar_em_servidores
                         )
        
    def get_remuneracao(self, email):
        return self.run(email)
            
    def obter_links_csv_mais_recentes(self):
        """
        Obtém os links dos CSVs mais recentes do portal da transparência.

        Parameters:
        - url_portal (str): URL do portal da transparência.
        - num_links (int): Número de links desejados.

        Returns:
        - list or str: Lista de hrefs ou mensagem de erro.
        """
        url_portal = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_REMUNERACOES
        response = self.http_client.post(url_portal)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Encontrar todos os links que correspondem ao padrão Remuneracoes-MM_YYYY.csv
            padrao = re.compile(r'remuneracao_(\w+)_(\d{4})\.rar', re.IGNORECASE)

            # Encontrar todos os links que correspondem ao padrão
            links = soup.find_all('a', href=padrao)

            conteudos_links = []

            # Função para converter o mês em número para fins de ordenação
            def obter_numero_mes(mes):
                meses = ['janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
                mes_minusculo_sem_acento = unidecode(mes.lower())
                return meses.index(mes_minusculo_sem_acento) + 1

            # Iterar sobre os links encontrados
            for link in links:
                conteudo = link.text.strip()  # Capturar o conteúdo e remover espaços em branco extras
                mes, ano = re.search(padrao, link['href']).groups()
                data_formatada = datetime.strptime(f'{obter_numero_mes(mes)}/{ano}', '%m/%Y').strftime('%Y%m')  # Formatar a data para ordenação
                conteudos_links.append((conteudo, data_formatada))

            # Ordenar a lista de conteúdos dentro das tags <a> de acordo com a ordem dos meses e anos
            links_ordenados = sorted(conteudos_links, key=lambda x: x[1], reverse=True)     
            
            return [links_ordenados[0][0]]
        else:
            self.print_api( f"Erro ao acessar a página. Status code: {response.status_code}")
            return None

    def extrair_data_do_link(self,link):
        """
        Extrai a data do link do CSV.

        Parameters:
        - link (str): Título do link.

        Returns:
        - int: Data no formato YYYYMM.
        """
        match = re.search(r'Remuneracoes-(\d{2})_(\d{4})\.csv', link)
        if match:
            return int(match.group(2) + match.group(1))
        else:
            return 0

    def ler_csv_e_transformar_em_servidores(self, lista_fontes_de_dados):
        """
        Lê o conteúdo do CSV e o transforma em um Dataframe.

        Parameters:
        - url (str): URL do CSV.

        Returns:
        - list or None: Lista de servidores ou None em caso de erro.
        """
        try:
            url = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_CSV.format(lista_fontes_de_dados[0])
            # Faz a requisição e obtém o conteúdo do CSV
            response = self.http_client.get(url)
            conteudo_csv = response.text

            # Usa o módulo StringIO para transformar a string em um objeto "file-like"
            arquivo_csv = StringIO(conteudo_csv)

            # criar um dataframe das remunerações
            df_remuneracoes = pd.read_csv(arquivo_csv, delimiter=';', usecols=[0, 1, 2, 3, 5], decimal=',')

            domains = self.get_cache_domains(df_remuneracoes.iloc[:, 2].unique().astype(str).tolist())
            df_domains = pd.DataFrame([vars(domain) for domain in domains])
            df_domains = df_domains.rename(columns={df_domains.columns[0]: 'ORGAO', df_domains.columns[1]: 'SIGLA', df_domains.columns[2]: 'DOMINIO'})

            df_remuneracoes = pd.merge(df_remuneracoes, df_domains, left_on= df_remuneracoes.iloc[:, 2], right_on="ORGAO")

            # Criar uma nova coluna com base na condição VantagemDesvantageme e na condição para "Rubrica"
            df_remuneracoes['SALARIO_TOTAL'] = df_remuneracoes.iloc[:, 3].fillna(0) + df_remuneracoes.iloc[:, 4].fillna(0)
            df_remuneracoes['REMUNERACAO_MENSAL_MEDIA'] = df_remuneracoes['SALARIO_TOTAL'].fillna(0) + df_remuneracoes['SALARIO_TOTAL'].fillna(0)/3/12 + df_remuneracoes['SALARIO_TOTAL'].fillna(0)/12 

            return df_remuneracoes[['NOME', 'REMUNERACAO_MENSAL_MEDIA', 'ORGAO', 'SIGLA', 'DOMINIO']]                                

        except Exception as e:
            self.print_api(f"Erro ao ler o CSV", e)
            return None
        
    
# Exemplo de utilização
if __name__ == "__main__":
    api = Api()
    servidor = api.get_remuneracao("fulano.tal@sp.gov.br")
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")
    servidor = api.get_remuneracao("ciclano.beltrano@sp.gov.br")
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")

     
