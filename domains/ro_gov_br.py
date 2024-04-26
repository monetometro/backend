"""
Este script realiza operações em dados de servidores públicos obtidos do portal da transparência do estado de Rondonia.
O objetivo geral é extrair o rendimento total de um servidor de um determinado órgão público. Como a maioria dos sites de 
transparencia fornecem dados no formato CSV, a opção 3.1 pode ser replicada para outros ambientes além do https://transparencia.ro.gov.br (Governo do RO).
A estratégia utilizada para arquivos CSV consiste em:
    1 - Identificar o arquivo CSV mensal de remunerações mais recente; [obter_links_csv_mais_recentes]
    2 - Extrair todos os registros de remuneração transformando-os em um dataframe; [ler_csv_e_transformar_em_servidores]
    3 - Buscar na lista de servidores por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [self.filter_by_email_login(email)]   
  

Autor: https://github.com/stgustavo
Data: 2023-11-22

"""

from bs4 import BeautifulSoup
from io import BytesIO
import pandas as pd
from commons.AbstractETL import AbstractETL 

# Constantes
URL_PORTAL_TRANSPARENCIA = "https://transparencia.ro.gov.br"
PATH_PORTAL_REMUNERACOES = "/pessoal"
PATH_PORTAL_CSV = "/Pessoal/Download?Ano={ano}&MesInicial={mes}&ContentTypeFile=EXCEL"

class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores do Estado de Rondonia.
    
    Domínio implementado: 
    - ro.gov.br   

    """   
    def __init__(self):
        super().__init__(dominio="ro.gov.br",
                         unidade_federativa="Rondonia",
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

        Returns:
        - list: Lista de hrefs ou mensagem de erro.
        """
        url_portal = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_REMUNERACOES
        response = self.http_client.get(url_portal)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            ano = soup.find('select', id='Ano').find('option', selected=True).text
            mes = soup.find('select', id='MesFinal').find('option', selected=True)['value']
            return [[ano, mes]]

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
            response = self.http_client.get(URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_CSV.format(ano=lista_fontes_de_dados[0][0],mes=lista_fontes_de_dados[0][1]))

            # Verifica se a requisição foi bem-sucedida
            if response.status_code == 200:
                # Lê o conteúdo do arquivo Excel baixado
                df_remuneracoes = pd.read_excel(BytesIO(response.content), decimal=',')
            else:
                return None 

            df_remuneracoes = df_remuneracoes.iloc[:, [0, 1, 3, 7]]
            df_remuneracoes = df_remuneracoes[df_remuneracoes.iloc[:, 2].str.lower() == "ativo"]
            df_remuneracoes.iloc[:, 3] = pd.to_numeric(df_remuneracoes.iloc[:, 3], errors="coerce")
            df_remuneracoes = df_remuneracoes.rename(columns={df_remuneracoes.columns[0]: 'NOME',df_remuneracoes.columns[1]: 'ORGAO',df_remuneracoes.columns[3]: 'SALARIO_TOTAL'})


            domains = self.get_cache_domains(df_remuneracoes.iloc[:, 1].unique().astype(str).tolist())
            df_domains = pd.DataFrame([vars(domain) for domain in domains])
            df_domains = df_domains.rename(columns={df_domains.columns[0]: 'ORGAO', df_domains.columns[1]: 'SIGLA', df_domains.columns[2]: 'DOMINIO'})

            df_resultado = pd.merge(df_remuneracoes, df_domains, left_on="ORGAO", right_on="ORGAO")
            df_resultado['REMUNERACAO_MENSAL_MEDIA'] = df_resultado.apply(lambda row: row['SALARIO_TOTAL'] + row['SALARIO_TOTAL']/3/12 + row['SALARIO_TOTAL']/12, axis=1)

            return df_resultado[['NOME', 'REMUNERACAO_MENSAL_MEDIA', 'ORGAO', 'SIGLA', 'DOMINIO']]      

        except Exception as e:
            self.print_api(f"Erro ao ler o CSV", e)
            return None
            
   
# Exemplo de utilização
if __name__ == "__main__":
    api = Api()
    servidor = api.get_remuneracao("fulano.tal@ro.gov.br")
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")
    servidor = api.get_remuneracao("ciclano.beltrano@ro.gov.br")
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")

     
