"""
Este script realiza operações em dados de servidores públicos obtidos do portal da transparência do Governo Federal.
O objetivo geral é extrair o rendimento total de um servidor do poder executivo federal. 
A estratégia utilizada para arquivos CSV consiste em:
    1 - Identificar o arquivo CSV mensal de remunerações mais recente; [obter_links_csv_mais_recentes]
    2 - Extrair todos os registros de remuneração transformando-os em objetos ServidorCSV; [ler_csv_e_transformar_em_servidores]
    3 - Buscar na lista de ServidorCSV por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha o domínio gov.br; [self.filter_by_email_login(email)]    
 
Autor: https://github.com/stgustavo
Data: 2024-02-18

"""

import re
import io
import zipfile
from io import StringIO 
import json
from commons.AbstractETL import AbstractETL 
import pandas as pd


# Constantes
URL_PORTAL_TRANSPARENCIA =  "https://portaldatransparencia.gov.br"
PATH_PORTAL_REMUNERACOES =  "/download-de-dados/servidores"
URL_PORTAL_CSV = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/servidores/{}_Servidores_{}.zip"

class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores do Executivo do Governo Federal.
    
    Domínio implementado: 
    - gov.br   

    """   
    def __init__(self):
        super().__init__(dominio="gov.br",
                        unidade_federativa="Governo Federal",
                        portal_remuneracoes_url=URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_REMUNERACOES,
                        fn_obter_link_mais_recente=self.obter_links_csv_mais_recentes,
                        fn_ler_fonte_de_dados_e_transformar_em_dataframe=self.ler_csv_e_transformar_em_servidores
                        )

        
    def get_remuneracao(self, email):
        return self.run(email)  


    def obter_links_csv_mais_recentes(self, check_siape=True):
        """
        Obtém os links dos CSVs mais recentes do portal da transparência.

        Parameters:
        - url_portal (str): URL do portal da transparência.
        - check_siape (bool): Flag para indicar qual a origem de dados vai consultar por padrão é SIAPE.

        Returns:
        - str: Ano e mês mais recente no formato AAAAMM ou None se não encontrado.
        """
        url_portal = self.portal_remuneracoes_url
        response = self.http_client.get(url_portal)

        if response.status_code == 200:
            # Encontrar o script que contém as informações de ano, mês e origem
            script_pattern = re.compile(r'arquivos\.push\(({.*?})\);', re.DOTALL)
            script_match = script_pattern.findall(response.text)
            anos_meses_validos = []

            if script_match:
                # Converter cada item para JSON e filtrar apenas os itens com "origem" igual a "Servidores_SIAPE"
                servidores = [json.loads(item) for item in script_match if json.loads(item).get('origem') == ("Servidores_SIAPE" if check_siape else "Servidores_BACEN")]

                lista_sistema_origem = ["SIAPE", "BACEN"]

                if servidores:
                    # Extrair o ano e mês de cada item
                    anos_meses = [(item.get('ano'), item.get('mes')) for item in servidores]

                    # Ordenar por ano e mês de forma decrescente
                    anos_meses.sort(reverse=True)

                    for ano, mes in anos_meses:
                        ano_mes_atual = f"{ano}{mes}"
                        if len(lista_sistema_origem) == 0:
                            break
                        for sistema_origem in lista_sistema_origem:
                            url = URL_PORTAL_CSV.format(ano_mes_atual, sistema_origem)
                            
                            # Fazer a solicitação HTTP HEAD
                            response = self.http_client.head(url)

                            # Verificar o código de status da resposta
                            if response.status_code == 200 and ((sistema_origem == "BACEN" and 
                                                                 int(response.headers.get('content-length', 0))>100*1024) or (sistema_origem == "SIAPE" and 
                                                                                                                              int(response.headers.get('content-length', 0))>50000*1024)):
                                lista_sistema_origem.remove(sistema_origem)
                                anos_meses_validos.append((ano_mes_atual, sistema_origem))
                                break  # Parar o loop assim que encontrar um ano_mes_atual válido
            return [anos_meses_validos] if anos_meses_validos != [] else None

        else:
            self.print_api(f"Erro ao acessar a página. Status code: {response.status_code}")
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
            lista_sistema_origem = ["SIAPE","BACEN"]
            servidores = pd.DataFrame()
            for sistema in lista_sistema_origem:
                valor_sistema = next((ano_mes for ano_mes, sistema_origem in lista_fontes_dados[0] if sistema_origem == sistema), None)
                url = URL_PORTAL_CSV.format(valor_sistema,sistema)
                # Faz a requisição e obtém o conteúdo do ZIP
                response = self.http_client.get(url)
                conteudo_zip = response.content

                # Abre o arquivo ZIP a partir do conteúdo
                with zipfile.ZipFile(io.BytesIO(conteudo_zip)) as zip_file:
                    # Procura por um arquivo que contenha a palavra "Remuneracao" e "Cadastro" no nome
                    conteudo_csv_remuneracao = None
                    conteudo_csv_cadastro = None

                    for nome_arquivo in zip_file.namelist():
                        if "Remuneracao" in nome_arquivo:
                            conteudo_csv_remuneracao = zip_file.read(nome_arquivo).decode('latin-1')                        
                        elif "Cadastro" in nome_arquivo:
                            conteudo_csv_cadastro = zip_file.read(nome_arquivo).decode('latin-1')

                    if conteudo_csv_remuneracao is None:
                        self.print_api("Nenhum arquivo 'Remuneracao' encontrado.")
                        return None

                    if conteudo_csv_cadastro is None:
                        self.print_api("Nenhum arquivo 'Cadastro' encontrado.")
                        return None

                    df_remuneracoes = pd.read_csv(StringIO(conteudo_csv_remuneracao), delimiter=';', usecols=[2, 4, 5, 15], decimal=',')

                    # Converter as colunas 5 e 15 para o tipo numérico
                    df_remuneracoes.iloc[:, 2] = pd.to_numeric(df_remuneracoes.iloc[:, 2], errors='coerce')
                    df_remuneracoes.iloc[:, 3] = pd.to_numeric(df_remuneracoes.iloc[:, 3], errors='coerce')

                    # Adiciona uma nova coluna contendo a soma das colunas 5 e 15
                    df_remuneracoes['SALARIO_TOTAL'] = df_remuneracoes.iloc[:, 2].fillna(0) + df_remuneracoes.iloc[:, 3].fillna(0)
                    
                    df_cadastros = pd.read_csv(StringIO(conteudo_csv_cadastro), delimiter=';', usecols=[0, 24])

                    # Join dos DataFrames usando a condição df_remuneracoes[2] == df_cadastros[0]
                    df_resultado = pd.merge(df_remuneracoes, df_cadastros, left_on=df_remuneracoes.columns[0], right_on=df_cadastros.columns[0])
                    df_resultado = df_resultado.rename(columns={df_resultado.columns[0]: 'ID', df_resultado.columns[1]: 'NOME', df_resultado.columns[4]: 'SALARIO_TOTAL', df_resultado.columns[5]: 'ORGAO'})

                    domains = self.get_cache_domains(df_resultado.iloc[:, 5].unique().astype(str).tolist())
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
    servidor = api.get_remuneracao("luiz.lula@gov.br")
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")
    else:
        api.print_api(servidor)
     
