"""
Este script realiza operações em dados de servidores públicos obtidos do portal da transparência do estado de Minas Gerais.
O objetivo geral é extrair o rendimento total de um servidor de um determinado órgão público. Como a maioria dos sites de 
transparencia fornecem dados no formato CSV, a opção 3.1 pode ser replicada para outros ambientes além do https://www.transparencia.mg.gov.br (Governo de SP).
A estratégia utilizada para arquivos CSV consiste em:
    1 - Identificar o arquivo CSV mensal de remunerações mais recente; [obter_links_csv_mais_recentes]
    2 - Extrair todos os registros de remuneração transformando-os em um dataframe; [ler_csv_e_transformar_em_servidores]
    3 - Buscar na lista de servidores por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [self.filter_by_email_login(email)]   
  

Autor: https://github.com/stgustavo
Data: 2024-03-10

"""

import re
from io import StringIO
import pandas as pd
from commons.AbstractETL import AbstractETL 

# Constantes
URL_PORTAL_TRANSPARENCIA = "https://www.transparencia.mg.gov.br"
PATH_PORTAL_REMUNERACOES = "/estado-pessoal/remuneracao-dos-servidores"
PATH_PORTAL_CSV = "/estado-pessoal/index.php?option=com_transparenciamg&task=estado_remuneracao.downloadRemuneracao&periodo={}"

class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores do Estado de Minas Gerais.
    
    Domínio implementado: 
    - mg.gov.br   

    """   
    def __init__(self):
        super().__init__(dominio="mg.gov.br",
                         unidade_federativa="Minas Gerais",
                         portal_remuneracoes_url = URL_PORTAL_TRANSPARENCIA+PATH_PORTAL_REMUNERACOES, 
                         fn_obter_link_mais_recente=self.obter_links_csv_mais_recentes,
                         fn_ler_fonte_de_dados_e_transformar_em_dataframe=self.ler_csv_e_transformar_em_servidores
                         )
        
    def get_remuneracao(self, email):
        return self.run(email)
            
    def obter_links_csv_mais_recentes(self):
        """
        Identifica o link do CSV mais recente do portal da transparência.

        Parameters:
        - url_portal (str): URL do portal da transparência.
        - num_links (int): Número de links desejados.

        Returns:
        - list or str: Lista de hrefs ou mensagem de erro.
        """

        url_portal = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_REMUNERACOES
        headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
                'Referer': f'{url_portal}',
            }
        response = self.http_client.get(url_portal, headers= headers)
        
        if response.status_code == 200:
            padrao = r'<strong>Período:</strong>\s*(\w+)/(\d{4})'
            match = re.search(padrao, response.text)
            if match:
                mes_str, ano = match.groups()
                meses = ['janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
                mes = str(meses.index(mes_str.lower()) + 1).zfill(2)  # Convertendo o nome do mês para número e preenchendo com zero à esquerda, se necessário
                return [mes + ano[-2:]]
            else:
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
            url = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_CSV.format(lista_fontes_de_dados[0])
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
                'Referer': f'{url}',
            }
            #req = request.Request(url, headers=headers)
            # Faz a requisição e obtém o conteúdo do CSV
            response = self.http_client.get(url)
            response.raise_for_status()  # Verifica se houve algum erro na solicitação
            conteudo_csv = response.text

            # Usa o módulo StringIO para transformar a string em um objeto "file-like"
            arquivo_csv = StringIO(conteudo_csv)

            # criar um dataframe das remunerações
            df_remuneracoes = pd.read_csv(arquivo_csv, delimiter=';', usecols=[1, 6, 9, 16], decimal=',')
            df_remuneracoes = df_remuneracoes.rename(columns={df_remuneracoes.columns[0]: 'NOME'})
            domains = self.get_cache_domains(df_remuneracoes.iloc[:, 1].unique().astype(str).tolist())
            df_domains = pd.DataFrame([vars(domain) for domain in domains])
            df_domains = df_domains.rename(columns={df_domains.columns[0]: 'ORGAO', df_domains.columns[1]: 'SIGLA', df_domains.columns[2]: 'DOMINIO'})

            df_remuneracoes = pd.merge(df_remuneracoes, df_domains, left_on= df_remuneracoes.iloc[:, 1], right_on="ORGAO")

            df_remuneracoes['SALARIO_TOTAL'] = df_remuneracoes.iloc[:, 2].fillna(0) 
            df_remuneracoes['REMUNERACAO_MENSAL_MEDIA'] = df_remuneracoes.iloc[:, 3].fillna(0) + df_remuneracoes['SALARIO_TOTAL'].fillna(0) + df_remuneracoes['SALARIO_TOTAL'].fillna(0)/3/12 + df_remuneracoes['SALARIO_TOTAL'].fillna(0)/12 

            return df_remuneracoes[['NOME', 'REMUNERACAO_MENSAL_MEDIA', 'ORGAO', 'SIGLA', 'DOMINIO']]                                

        except Exception as e:
            self.print_api(f"Erro ao ler o CSV", e)
            return None
        
    
# Exemplo de utilização
if __name__ == "__main__":
    api = Api()
    servidor = api.get_remuneracao("fulano.tal@mg.gov.br")
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")
    servidor = api.get_remuneracao("ciclano.beltrano@mg.gov.br")
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")

     
