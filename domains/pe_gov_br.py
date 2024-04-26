"""
Este script realiza operações em dados de servidores públicos obtidos do portal da transparência do estado de Pernambuco.
O objetivo geral é extrair o rendimento total de um servidor de um determinado órgão público. Como a maioria dos sites de 
transparencia fornecem dados no formato CSV, a opção 3.1 pode ser replicada para outros ambientes além do https://transparencia.pe.gov.br.
A estratégia utilizada para arquivos CSV consiste em:
    1 - Identificar o arquivo CSV mensal de remunerações mais recente; [obter_links_csv_mais_recentes]
    2 - Extrair todos os registros de remuneração transformando-os em um dataframe; [ler_csv_e_transformar_em_servidores]
    3 - Buscar na lista de servidores por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [self.filter_by_email_login(email)]   
  

Autor: https://github.com/stgustavo
Data: 2024-03-10

"""

from bs4 import BeautifulSoup
import re
import os
from io import StringIO
import pandas as pd
from commons.AbstractETL import AbstractETL 

# Constantes
URL_PORTAL_TRANSPARENCIA = "https://dados.pe.gov.br"
PATH_PORTAL_REMUNERACOES = "/dataset/remuneracao-de-servidores"
PATH_PORTAL_CSV = "/dataset/remuneracao-de-servidores/resource/{}"

class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores do Estado de Pernambuco.
    
    Domínio implementado: 
    - pe.gov.br   

    """   
    def __init__(self):
        super().__init__(dominio="pe.gov.br",
                         unidade_federativa="Pernambuco",
                         portal_remuneracoes_url=URL_PORTAL_TRANSPARENCIA+PATH_PORTAL_REMUNERACOES,
                         fn_obter_link_mais_recente=self.obter_links_csv_mais_recentes,
                         fn_ler_fonte_de_dados_e_transformar_em_dataframe=self.ler_csv_e_transformar_em_servidores
                         )
        

    def get_remuneracao(self, email):
        return self.run(email)           
            
    
    def obter_links_csv_mais_recentes(self, num_links=1):
        """
        Obtém os links dos CSVs mais recentes do portal da transparência.

        Parameters:
        - url_portal (str): URL do portal da transparência.
        - num_links (int): Número de links desejados.

        Returns:
        - list or str: Lista de hrefs ou mensagem de erro.
        """
        url_portal = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_REMUNERACOES
        response = self.http_client.get(url_portal)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
                        
            # Encontrar todos os elementos 'a' que tenham data-format="csv" e title começando com "Remuneração ativos"
            links = soup.find_all(lambda tag: tag.name == 'a' and tag.get('title') and tag.get('title').startswith('Remuneração ativos') and 'data-format="csv"' in str(tag))

            # Ordenar os links com base no título
            links_csv = sorted(
                    links,
                    key=lambda x: tuple(map(int, re.search(r'(\d{2})/(\d{4})$', x['title']).groups()[::-1])) if re.search(r'(\d{2})/(\d{4})$', x['title']) else (),
                    reverse=True
                )
            # Validar se existem pelo menos num_links antes de tentar retornar
            if len(links_csv) >= num_links:
                # Extrair os href relativos correspondentes aos títulos mais recentes
                hrefs_mais_recentes = [os.path.basename(link['href']) for link in links_csv[:num_links]]
                return hrefs_mais_recentes
            else:
                self.print_api("Menos de 3 links encontrados com o padrão desejado.")
                return None
        else:
            self.print_api( f"Erro ao acessar a página. Status code: {response.status_code}")
            return None


    def ler_csv_e_transformar_em_servidores(self, lista_fontes_de_dados):
        """
        Lê o conteúdo do CSV e o transforma em um Dataframe.

        Parameters:
        - url (str): URL do CSV.

        Returns:
        - list or None: Lista de servidores ou None em caso de erro.
        """
        try:
            
            url_portal = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_CSV.format(lista_fontes_de_dados[0])
            response = self.http_client.get(url_portal)
        
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                            
                # Encontrar todos os elementos 'a' que tenham data-format="csv" e title começando com "Remuneração ativos"
                links = [link['href'] for link in soup.find_all(lambda tag: tag.name == 'a' and any(child.string and 'Baixar' in child.string for child in tag.children))]
                url_portal = links[0]


            # Faz a requisição e obtém o conteúdo do CSV
            response = self.http_client.get(url_portal)
            conteudo_csv = response.text

            # Usa o módulo StringIO para transformar a string em um objeto "file-like"
            arquivo_csv = StringIO(conteudo_csv)

            # criar um dataframe das remunerações
            df_remuneracoes = pd.read_csv(arquivo_csv, delimiter=';', usecols=[0, 3, 9, 12], decimal='.')

            df_remuneracoes = df_remuneracoes.rename(columns={df_remuneracoes.columns[0]: 'ORGAO',df_remuneracoes.columns[1]: 'NOME', df_remuneracoes.columns[2]: 'SALARIO', df_remuneracoes.columns[3]: 'OUTROS'})
            domains = self.get_cache_domains(df_remuneracoes.iloc[:, 0].unique().astype(str).tolist())
            df_domains = pd.DataFrame([vars(domain) for domain in domains])
            df_domains = df_domains.rename(columns={df_domains.columns[0]: 'ORGAO', df_domains.columns[1]: 'SIGLA', df_domains.columns[2]: 'DOMINIO'})

            df_remuneracoes = pd.merge(df_remuneracoes, df_domains, left_on= df_remuneracoes.iloc[:, 0], right_on="ORGAO")

            df_remuneracoes['SALARIO_TOTAL'] = df_remuneracoes.iloc[:, 3].fillna(0) + df_remuneracoes.iloc[:, 4].fillna(0) 
            df_remuneracoes['REMUNERACAO_MENSAL_MEDIA'] = df_remuneracoes['SALARIO_TOTAL'].fillna(0) + df_remuneracoes['SALARIO_TOTAL'].fillna(0)/3/12 + df_remuneracoes['SALARIO_TOTAL'].fillna(0)/12 

            return df_remuneracoes[['NOME', 'REMUNERACAO_MENSAL_MEDIA', 'ORGAO', 'SIGLA', 'DOMINIO']]                                


        except Exception as e:
            self.print_api(f"Erro ao ler o CSV", e)
            return None
            
   
# Exemplo de utilização
if __name__ == "__main__":
    api = Api()
    servidor = api.get_remuneracao("fulano.tal@es.gov.br")
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")
    servidor = api.get_remuneracao("ciclano.beltrano@es.gov.br")
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")

     
