"""
Este script realiza operações em dados de servidores públicos obtidos do portal da dados abertos do estado do Espírito Santo.
O objetivo geral é extrair o rendimento total de um servidor do tribunal de contas do ES. 
O Tribunal disponibiliza um link estático que fornece os dados mais atualizados de remuneração dos seus servidores.
A estratégia utilizada para arquivos CSV consiste em:
    1 - Identificar a competencia mais recente; [get_competencia_mais_recente]
    2 - Extrair todos os registros de remuneração transformando-os em um dataframe; [ler_csv_e_transformar_em_servidores]
    3 - Buscar na lista de servidores por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [self.filter_by_email_login(email)]  

Autor: https://github.com/stgustavo
Data: 2023-01-01

"""

from io import StringIO
import pandas as pd
from commons.AbstractETL import AbstractETL 

# Constantes
URL_PORTAL_TRANSPARENCIA = "https://dados.es.gov.br"
PATH_PORTAL_SQL = '/api/3/action/datastore_search?q={nome}{sobrenome}&resource_id={guid}'
PATH_PORTAL_CSV = '/datastore/dump/{guid}?bom=True'
GUID_DATASOURCE = 'f07af7e6-80f1-4726-b938-632123dfe30e'

class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores Tribunal de Contas do Estado do Espírito Santo.
    
    Domínio implementado: 
    - tcees.tc.br   

    """   
    def __init__(self):
        super().__init__(dominio="tcees.tc.br",
                         unidade_federativa="Espírito Santo",
                        portal_remuneracoes_url=URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_SQL.format(guid=GUID_DATASOURCE, nome='', sobrenome=''),
                        fn_obter_link_mais_recente=self.get_competencia_mais_recente,
                        fn_ler_fonte_de_dados_e_transformar_em_dataframe=self.ler_csv_e_transformar_em_servidores
                        )


    def get_remuneracao(self, email):
        return self.run(email)  


    def get_competencia_mais_recente(self):
        guid_arquivo = GUID_DATASOURCE
        # Construa a URL completa
        url_completa = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_SQL.format(guid=guid_arquivo, nome='',sobrenome='')

        # Faça a chamada à API
        response = self.http_client.get(url_completa)

        # Verifique se a solicitação foi bem-sucedida (código de status 200)
        if response.status_code == 200:
            dados_api = response.json()

            return [max((registro["Competencia"] for registro in dados_api["result"]["records"]), default=None)]
        return ['00/0000']

    def ler_csv_e_transformar_em_servidores(self, lista_fontes_de_dados):
        """
        Lê o conteúdo do CSV e o transforma em um Dataframe.

        Parameters:
        - url (str): URL do CSV.

        Returns:
        - list or None: Lista de servidores ou None em caso de erro.
        """
        try:
            url = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_CSV.format(guid=GUID_DATASOURCE)
            max_competencia = lista_fontes_de_dados[0]
            # Faz a requisição e obtém o conteúdo do CSV
            response = self.http_client.get(url)
            conteudo_csv = response.text

            # Usa o módulo StringIO para transformar a string em um objeto "file-like"
            arquivo_csv = StringIO(conteudo_csv)

            # criar um dataframe das remunerações
            df_remuneracoes = pd.read_csv(arquivo_csv, delimiter=',', usecols=[1, 6, 7, 9, 11], decimal=',')
            df_remuneracoes = df_remuneracoes[df_remuneracoes['Competencia'] == max_competencia]
            # Criar uma nova coluna com base na condição TipoEventro e na condição para "DescricaoEvento"
            df_remuneracoes['REMUNERACAO_MENSAL_MEDIA'] = df_remuneracoes.apply(lambda row: 0 if any(substring in row["DescricaoEvento"] for substring in ["DECIMO TERCEIRO", "13", " FER"]) else (row.iloc[4] + row.iloc[4]/3/12 + row.iloc[4]/12 ) if row.iloc[2].lower() == 'c' else 0 if pd.notna(row.iloc[2]) else 0, axis=1)

            df_remuneracoes = df_remuneracoes.drop(columns=["DescricaoEvento"])

            # Criar um DataFrame agrupado
            df_agrupado = df_remuneracoes.groupby([df_remuneracoes.iloc[:, 0]]).agg({
                'REMUNERACAO_MENSAL_MEDIA': 'sum',
            }).reset_index()

            df_agrupado['ORGAO'] = 'Tribunal de Contas do Estado do Espírito Santo'
            df_agrupado['SIGLA'] = 'TCEES'
            df_agrupado['DOMINIO'] = self.dominio

            return df_agrupado

        except Exception as e:
            self.print_api("Erro ao ler o CSV", e)
            return None
            

# Exemplo de utilização
if __name__ == "__main__":
    api = Api()
    servidor = api.get_remuneracao_por_tipo_origem("fulano.tal@tcees.tc.br", False)
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")
    servidor = api.get_remuneracao_por_tipo_origem("ciclano.beltrano@tcees.tc.br", False)
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")

     
