"""
Este script realiza operações em dados de servidores públicos obtidos do portal da transparência do estado do Espírito Santo.
O objetivo geral é extrair o rendimento total de um servidor de um determinado órgão público. Como a maioria dos sites de 
transparencia fornecem dados no formato CSV, a opção 3.1 pode ser replicada para outros ambientes além do https://transparencia.pr.gov.br (Governo do PR).
A estratégia utilizada para arquivos CSV consiste em:
    1 - Identificar o arquivo CSV mensal de remunerações mais recente; [obter_links_csv_mais_recentes]
    2 - Extrair todos os registros de remuneração transformando-os em um dataframe; [ler_csv_e_transformar_em_servidores]
    3 - Buscar na lista de servidores por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [self.filter_by_email_login(email)]   
  

Autor: https://github.com/stgustavo
Data: 2024-03-17

"""

import io
import zipfile
from io import StringIO
import pandas as pd
from commons.AbstractETL import AbstractETL 
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# Constantes
URL_PORTAL_TRANSPARENCIA = "https://www.transparencia.pr.gov.br"
PATH_PORTAL_REMUNERACOES = "/pte/pessoal/servidores/poderexecutivo/remuneracao"
PATH_PORTAL_RH = "https://www.transparencia.download.pr.gov.br/exportacao/REMUNERACAO_RH/REMUNERACAO_RH.zip"
PATH_PORTAL_RH_REMUNERACOES = "https://www.transparencia.download.pr.gov.br/exportacao/REMUNERACAO/REMUNERACAO"


class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores do Estado do Paraná.
    
    Domínio implementado: 
    - pr.gov.br   

    """   
    def __init__(self):
        super().__init__(dominio="pr.gov.br",
                         unidade_federativa="Paraná",
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
        url_encontrada = self.procurar_arquivo_zip(PATH_PORTAL_RH_REMUNERACOES, num_meses=12)

        return [url_encontrada]
    

    def verificar_existencia_arquivo(self, url):
        try:
            response = self.http_client.head(url, allow_redirects=True)
            
            # Verificar o código de status final após os redirecionamentos
            if response.history:
                last_response = response.history[-1]
            else:
                last_response = response

            return url if last_response.status_code == 200 else None
        except Exception as e:
            print(f"Erro ao consultar ZIP {url}: {e}")
    

    def procurar_arquivo_zip(self, base_url, num_meses=12):
        data_atual = datetime.now().date()
        urls = []

        with ThreadPoolExecutor() as executor:
            for _ in range(num_meses):
                mes_atual = data_atual.strftime("%Y-%m")
                url = f"{base_url}-{mes_atual}.zip"
                urls.append(url)
                data_atual -= timedelta(days=data_atual.day)

            # Fazendo as requisições HTTP em paralelo
            resultados = executor.map(self.verificar_existencia_arquivo, urls)

        # Retorna a URL com o mês e ano mais recente que contém o arquivo ZIP
        for resultado in resultados:
            if resultado:
                return resultado

        # Se nenhum arquivo foi encontrado
        return None



    def ler_csv_e_transformar_em_servidores(self, lista_fontes_dados):
        """
        Lê o conteúdo do CSV e o transforma em um Dataframe.

        Parameters:
        - url (str): URL do ZIP contendo o CSV.

        Returns:
        - list or None: Lista de servidores ou None em caso de erro.
        """
        try:
            servidores = pd.DataFrame()
                
            # Faz a requisição e obtém o conteúdo do ZIP
            response = self.http_client.get(PATH_PORTAL_RH)
            conteudo_zip_rh = response.content
            response = self.http_client.get(lista_fontes_dados[0])
            conteudo_zip_remuneracao = response.content

            conteudo_csv_remuneracao = None
            conteudo_csv_cadastro = None    

            # Abre o arquivo ZIP a partir do conteúdo
            with zipfile.ZipFile(io.BytesIO(conteudo_zip_remuneracao)) as zip_file:
                for nome_arquivo in zip_file.namelist():
                    conteudo_csv_remuneracao = zip_file.read(nome_arquivo).decode('latin-1')                        
            
            with zipfile.ZipFile(io.BytesIO(conteudo_zip_rh)) as zip_file:
                for nome_arquivo in zip_file.namelist():
                    conteudo_csv_cadastro = zip_file.read(nome_arquivo).decode('latin-1')                        

            if conteudo_csv_remuneracao is None:
                self.print_api("Nenhum arquivo 'Remuneracao' encontrado.")
                return None

            if conteudo_csv_cadastro is None:
                self.print_api("Nenhum arquivo 'Cadastro' encontrado.")
                return None

            df_remuneracoes = pd.read_csv(StringIO(conteudo_csv_remuneracao), delimiter=';', usecols=[0, 7, 8], decimal='.')

            # Converter as colunas 5 e 15 para o tipo numérico
            df_remuneracoes.iloc[:, 1] = pd.to_numeric(df_remuneracoes.iloc[:, 1], errors='coerce')
            df_remuneracoes.iloc[:, 2] = pd.to_numeric(df_remuneracoes.iloc[:, 2], errors='coerce')

            # Adiciona uma nova coluna contendo a soma das colunas 5 e 15
            df_remuneracoes['SALARIO_TOTAL'] = df_remuneracoes.iloc[:, 2].fillna(0) - df_remuneracoes.iloc[:, 1].fillna(0)
            
            df_cadastros = pd.read_csv(StringIO(conteudo_csv_cadastro), delimiter=';', usecols=[0, 1, 2, 3])

            # Join dos DataFrames usando a condição df_remuneracoes[2] == df_cadastros[0]
            df_resultado = pd.merge(df_remuneracoes, df_cadastros, left_on=df_remuneracoes.columns[0], right_on=df_cadastros.columns[0])
            df_resultado = df_resultado.rename(columns={df_resultado.columns[0]: 'ID', df_resultado.columns[4]: 'NOME', df_resultado.columns[6]: 'ORGAO'})

            domains = self.get_cache_domains(df_resultado.iloc[:, 6].unique().astype(str).tolist())
            df_domains = pd.DataFrame([vars(domain) for domain in domains])
            df_domains = df_domains.rename(columns={df_domains.columns[0]: 'ORGAO', df_domains.columns[1]: 'SIGLA', df_domains.columns[2]: 'DOMINIO'})

            df_resultado = pd.merge(df_resultado, df_domains, left_on="ORGAO", right_on="ORGAO")
            df_resultado['REMUNERACAO_MENSAL_MEDIA'] = df_resultado.apply(lambda row: row['SALARIO_TOTAL'] + row['SALARIO_TOTAL']/3/12 + row['SALARIO_TOTAL']/12, axis=1)

            resultado = df_resultado[['NOME', 'REMUNERACAO_MENSAL_MEDIA', 'ORGAO', 'SIGLA', 'DOMINIO']]                                
            if not servidores.empty:
                servidores = pd.concat([servidores, resultado], ignore_index=True)
            else:
                servidores = resultado
        
            return servidores

        except Exception as e:
            self.log(f"Erro ao ler o ZIP: {e}")
            return None
            
   
# Exemplo de utilização
if __name__ == "__main__":
    api = Api()
    servidor = api.get_remuneracao(f"fulano.tal@pr.gov.br")
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")
    servidor = api.get_remuneracao(f"ciclano.beltrano@pr.gov.br")
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")

     
