"""
Este script realiza operações em dados de servidores públicos obtidos do portal da transparência do estado do Pará.
O objetivo geral é extrair o rendimento total de um servidor de um determinado órgão público. Como a maioria dos sites de 
transparencia fornecem dados no formato CSV, a opção 3.1 pode ser replicada para outros ambientes além do https://transparencia.ms.gov.br (Governo do MS)
A estratégia utilizada para arquivos CSV consiste em:
    1 - Identificar o arquivo CSV mensal de remunerações mais recente; [obter_links_csv_mais_recentes]
    2 - Extrair todos os registros de remuneração transformando-os em um dataframe; [ler_csv_e_transformar_em_servidores]
    3 - Buscar na lista de servidores por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [self.filter_by_email_login(email)]   
  

Autor: https://github.com/stgustavo
Data: 2023-03-21

"""

import pandas as pd
from commons.AbstractETL import AbstractETL 
from datetime import datetime


# Constantes
URL_PORTAL_TRANSPARENCIA = "http://www.transparencia.ms.gov.br"
PATH_PORTAL_REMUNERACOES = "/#/Servidores"
URL_PORTAL_API = 'http://api.sgi.ms.gov.br/d0125/transpfolhadepagamento/v1/servidores?anoexercicio={ano}&cargo=&cpf=&exportarcsv=false&mescompetencia={mes}&nome=&orgao=&pageno=1&pagesize={tamanho}&situacao=&tipoFolha=0&vinculo='
URL_DETALHES_API="https://ptp-api-dados.sistemas.ms.gov.br/dados-transparencias/funcionarios/detalhes?ano={ano}&mes={mes}&funcionario={id}"
URL_GET_TOKEN="https://id.ms.gov.br/auth/realms/ms/protocol/openid-connect/token"
GET_TOKEN_PAYLOAD = {
            "client_id": "ptransparencia.app",
            "grant_type": "client_credentials",
            "client_secret": "95064d80-afd5-4e6b-8cb5-4385db664f5a"
        }

class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores do Estado do Mato Grosso do Sul.
    
    Domínio implementado: 
    - ms.gov.br   

    """   
    def __init__(self):
        super().__init__(dominio="ms.gov.br",
                         unidade_federativa="Mato Grosso do Sul",
                         portal_remuneracoes_url=URL_PORTAL_TRANSPARENCIA+PATH_PORTAL_REMUNERACOES,
                         fn_obter_link_mais_recente=self.obter_links_api_mais_recentes,
                         fn_ler_fonte_de_dados_e_transformar_em_dataframe=self.ler_api_e_transformar_em_servidores
                         )
        

    def get_remuneracao(self, email):
        return self.run(email)           
            
    
    def obter_links_api_mais_recentes(self):
        """
        Obtém os links das APIs mais recentes do portal da transparência.

        Parameters:
        - url_portal (str): URL do portal da transparência.

        Returns:
        - list: Lista de hrefs ou mensagem de erro.
        """
        ano_atual = datetime.now().year
        mes_atual = datetime.now().month

        headers = {
            "Authorization": "Bearer " + self.obter_access_token()
        }

        for i in range(12):
            ano = ano_atual
            mes = mes_atual - i
            if mes <= 0:
                ano -= 1
                mes += 12

            url_com_data = URL_PORTAL_API.format(ano=str(ano), mes=str(mes), tamanho="1")
            
            response = self.http_client.get(url_com_data, headers=headers)
            if response.status_code == 200:
                # Se a resposta for 200, retorna a URL correspondente
                return [URL_PORTAL_API.format(ano=str(ano), mes=str(mes), tamanho="500000")]
            elif response.status_code == 404:
                # Se a resposta for 404, e já tentou decrementar por 12 meses, encerra o loop
                if i == 11:
                    break
                # Se não, continue procurando com o próximo mês
                continue
            else:
                # Se a resposta for diferente de 200 e 404, algo inesperado aconteceu, encerre o loop
                print(f"Erro inesperado ao fazer a requisição: {response.status_code}")
                break
                        
        return None
 


    def ler_api_e_transformar_em_servidores(self, lista_fontes_de_dados):
        """
        Lê o conteúdo da API e o transforma em um Dataframe.

        Parameters:
        - url (str): URL da API.

        Returns:
        - list or None: Lista de servidores ou None em caso de erro.
        """
        try:
            servidores = pd.DataFrame()
            headers = {
                "Authorization": "Bearer " + self.obter_access_token()
            }
            response = self.http_client.get(lista_fontes_de_dados[0],headers=headers)
            if response.status_code == 200:
                data = response.json()['data']
                if data:
                    df = pd.DataFrame(data)
                    # Filtrando as colunas necessárias
                    df = df[["orgao", "matricula", "remuneracaoFixa", "nome"]]
                    df = df.rename(columns={df.columns[0]: 'ORGAO',df.columns[2]: 'SALARIO_TOTAL',df.columns[3]: 'NOME'})

                    domains = self.get_cache_domains(df.iloc[:, 0].unique().astype(str).tolist())
                    df_domains = pd.DataFrame([vars(domain) for domain in domains])
                    df_domains = df_domains.rename(columns={df_domains.columns[0]: 'ORGAO', df_domains.columns[1]: 'SIGLA', df_domains.columns[2]: 'DOMINIO'})

                    df_resultado = pd.merge(df, df_domains, left_on="ORGAO", right_on="ORGAO")
                    df_resultado['REMUNERACAO_MENSAL_MEDIA'] = df_resultado.apply(lambda row: row['SALARIO_TOTAL'] + row['SALARIO_TOTAL']/3/12 + row['SALARIO_TOTAL']/12, axis=1)

                    resultado = df_resultado[['NOME', 'REMUNERACAO_MENSAL_MEDIA', 'ORGAO', 'SIGLA', 'DOMINIO']]                                
                    if not servidores.empty:
                        servidores = pd.concat([servidores, resultado], ignore_index=True)
                    else:
                        servidores = resultado
                
                    return servidores
                else:
                    self.print_api("Nenhum dado retornado pela API.")
                    return None
            else:
                print("Erro ao obter dados da API:", response.status_code)
                return None
        except Exception as e:
            print("Erro ao obter dados da API:", e)
            return None

    def obter_access_token(self):
        url = URL_GET_TOKEN
        payload = GET_TOKEN_PAYLOAD
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        try:
            response = self.http_client.post(url, data=payload, headers=headers)
            if response.status_code == 200:
                return response.json().get("access_token")
            else:
                print("Erro ao obter access token:", response.status_code)
                return None
        except Exception as e:
            print("Erro ao obter access token:", e)
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

     
