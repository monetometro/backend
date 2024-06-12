"""
Este script realiza operações em dados de servidores públicos obtidos do portal da transparência do estado do Espírito Santo.
O objetivo geral é extrair o rendimento total de um servidor de um determinado órgão público. Como a maioria dos sites de 
transparencia fornecem dados no formato CSV, a opção 3.1 pode ser replicada para outros ambientes além do https://transparencia.es.gov.br (Governo do ES).
A estratégia utilizada para arquivos CSV consiste em:
    1 - Identificar o arquivo CSV mensal de remunerações mais recente; [obter_links_csv_mais_recentes]
    2 - Extrair todos os registros de remuneração transformando-os em um dataframe; [ler_csv_e_transformar_em_servidores]
    3 - Buscar na lista de servidores por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [self.filter_by_email_login(email)]   
  

Autor: https://github.com/stgustavo
Data: 2023-11-22

"""

from bs4 import BeautifulSoup
import re
import os
from urllib import request
from io import StringIO
import pandas as pd
from commons.AbstractETL import AbstractETL 

# Constantes
URL_PORTAL_TRANSPARENCIA = "https://dados.es.gov.br"
PATH_PORTAL_REMUNERACOES = "/dataset/portal-da-transparencia-pessoal"
PATH_PORTAL_CSV = "/datastore/dump/{}?bom=True"
PATH_PORTAL_SQL = '/api/3/action/datastore_search?q={nome}%{sobrenome}%{orgao}&resource_id={guid}'

class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores do Estado do Espírito Santo.
    
    Domínio implementado: 
    - es.gov.br   

    """   
    def __init__(self):
        super().__init__(dominio="es.gov.br",
                         unidade_federativa="Espírito Santo",
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
        - num_links (int): Número de links desejados, por padrão retorna 1 (um), o mais recente.

        Returns:
        - list: Lista de hrefs ou mensagem de erro.
        """
        url_portal = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_REMUNERACOES
        response = self.http_client.get(url_portal)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Encontrar todos os links que correspondem ao padrão Remuneracoes-MM_YYYY.csv
            links_csv = [a for a in soup.find_all('a', title=re.compile(r'Remuneracoes-\d{2}_\d{4}\.csv'))]
            links_csv.sort(key=lambda a: self.extrair_data_do_link(a['title']), reverse=True)
            
            # Validar se existem pelo menos num_links antes de tentar retornar
            if len(links_csv) >= num_links:
                # Extrair os href relativos correspondentes aos títulos mais recentes
                hrefs_mais_recentes = [os.path.basename(link['href']) for link in links_csv[:num_links]]
                return hrefs_mais_recentes
            else:
                return None
        else:
            self.print_api(f"Erro ao acessar a página. Status code: {response.status_code}")
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

            # Faz a requisição e obtém o conteúdo do CSV
            response = self.http_client.get(URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_CSV.format(lista_fontes_de_dados[0]))
                
            # Usa o módulo StringIO para transformar a string em um objeto "file-like"
            arquivo_csv = StringIO(response.text)

            # criar um dataframe das remunerações
            df_remuneracoes = pd.read_csv(arquivo_csv, delimiter=',', usecols=[1, 4, 17, 19, 16], decimal=',')

            # Criar uma nova coluna com base na condição VantagemDesvantageme e na condição para "Rubrica"
            df_remuneracoes['REMUNERACAO_MENSAL_MEDIA'] = df_remuneracoes.apply(lambda row: 0 if any(substring in row["Rubrica"] for substring in ["DECIMO TERCEIRO", "13", " FER"]) else (row.iloc[4] + row.iloc[4]/3/12 + row.iloc[4]/12 ) if row.iloc[3].lower() == 'v' else 0 if pd.notna(row.iloc[3]) else 0, axis=1)

            df_remuneracoes = df_remuneracoes.drop(columns=["Rubrica"])

            # Criar um DataFrame agrupado
            df_agrupado = df_remuneracoes.groupby([df_remuneracoes.iloc[:, 0], df_remuneracoes.iloc[:, 1]]).agg({
                'REMUNERACAO_MENSAL_MEDIA': 'sum',
            }).reset_index()

            df_agrupado['SIGLA'] = df_agrupado.iloc[:, 0].fillna(0).str.lower()
            #df_agrupado['DOMINIO'] = df_agrupado.iloc[:, 0].fillna(0).str.lower() + ".es.gov.br"

            domains = self.get_cache_domains(df_agrupado.iloc[:, 0].unique().astype(str).tolist())
            df_domains = pd.DataFrame([vars(domain) for domain in domains])
            df_domains = df_domains.rename(columns={df_domains.columns[0]: 'ORGAO', df_domains.columns[1]: 'SIGLA', df_domains.columns[2]: 'DOMINIO'})

            df_agrupado = pd.merge(df_agrupado, df_domains, left_on= df_agrupado.iloc[:, 0], right_on="ORGAO")

            return df_agrupado

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

     
