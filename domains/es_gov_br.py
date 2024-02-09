"""
Este script realiza operações em dados de servidores públicos obtidos do portal da transparência do estado do Espírito Santo.
O objetivo geral é extrair o rendimento total de um servidor de um determinado órgão público. Como a maioria dos sites de 
transparencia fornecem dados no formato CSV, a opção 3.1 pode ser replicada para outros ambientes além do https://transparencia.es.gov.br (Governo do ES).
A estratégia utilizada para arquivos CSV consiste em:
    1 - Identificar o arquivo CSV mensal de remunerações mais recente; [obter_links_csv_mais_recentes]
    2 - Extrair todos os registros de remuneração transformando-os em objetos ServidorCSV; [ler_csv_e_transformar_em_servidores]
    3 - Buscar na lista de ServidorCSV por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [filtrar_e_agrupar_servidores_por_email]    
        Como no site da transparência existe a possibilidade de fazer consultas direcionadas via API de Dados por consulta SQL foi selecionado essa opção como 
        padrão para retorno mais performático dos dados necessários. Para esse caminho a estratégia sosiste em:

        1 - Identificar o arquivo CSV mensal de remunerações mais recente, pois contem a chave de identificação da base de dados da rota da API; [obter_links_csv_mais_recentes]
        2 - Buscar, via filtro por consulta API, os objetos ServidorCSV por um determinado email, para isso infere-se que a parte de login no email contenha 
            o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [filtrar_e_agrupar_via_api_servidores_por_email]    

Autor: https://github.com/stgustavo
Data: 2023-11-22

"""

import requests
from bs4 import BeautifulSoup
import re
import os
import csv
from urllib import request
from io import StringIO
from commons.ServidorCSV import ServidorCSV
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
        super().__init__("es.gov.br",URL_PORTAL_TRANSPARENCIA+PATH_PORTAL_REMUNERACOES)
        
    def get_remuneracao(self, email):
        servidor= self.get_remuneracao_por_tipo_origem(email, True)            
        if servidor == None:
            self.print_api("Nenhum servidor encontrado com base no email.")
        else:
            servidor.email = email    
            return servidor
            

    def get_remuneracao_por_tipo_origem(self, email, origem_eh_api=True):
        """
        Retorna um objeto ServidorCSV do portal da transparência baseado no email recebido.

        Parameters:
        - email (str): Endereço de email pertencente ao domínio es.gov.br.
        - origem_api (bool): Indicativo de qual método de filtragem dos dados será utilizado, se via API ou arquivo CSV.

        Returns:
        - ServidorCSV: Uma classe ServidorCSV preenchida ou caso não encontre retornará None.  
        """
        guids_mais_recentes = self.obter_links_csv_mais_recentes(URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_REMUNERACOES)
        if isinstance(guids_mais_recentes, list):
   
            # Inicializa um dicionário para armazenar os resultados agrupados
            resultados_agrupados = {}
            
            servidores = self.get_database_by_link(guids_mais_recentes) or []

            # Itera sobre cada arquivo mais recente e executa a rotina
            if (servidores==[]):
                # Itera sobre cada guid mais recente e executa a rotina
                for guid in guids_mais_recentes:
                    if  origem_eh_api:
                        servidores = self.filtrar_e_agrupar_via_api_servidores_por_email(email, guid)
                        servidor = ServidorCSV(orgao=servidores[0].orgao, nome=servidores[0].nome, valor=servidores[0].valor) if servidores else None
                        if servidor != None:
                            self.print_api(f"Orgao: {servidor.orgao}, Nome: {servidor.nome}, Remuneração: {servidor.valor}")
                        return servidor
                    else:
                        servidores = self.ler_csv_e_transformar_em_servidores(URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_CSV.format(guid))

                        # Itera sobre cada servidor e ajusta o valor, porque pode existir mais de uma linha para remuneração
                        for servidor in servidores:
                            chave = (servidor.orgao, servidor.nome)

                            # Agrupa os resultados para cada execução e adiciona ao dicionário
                            resultado_agrupado = self.filtrar_e_agrupar_servidores_por_email(email, servidores)
                            for chave, valor in resultado_agrupado.items():
                                if chave in resultados_agrupados:
                                    resultados_agrupados[chave] += valor
                                else:
                                    resultados_agrupados[chave] = valor
                
                self.add_to_database(arquivos_mais_recentes, servidores)

            if resultados_agrupados:
                for chave, valor in resultados_agrupados.items():
                    orgao, nome = chave
                    servidor = ServidorCSV(orgao=orgao, nome=nome, valor=valor)
                    if servidor != None:
                        self.print_api(f"Orgao: {servidor.orgao}, Nome: {servidor.nome}, Remuneração: {servidor.valor}")
                    return servidor
            else:
                return None

        else:
            return None

    def obter_links_csv_mais_recentes(self, url_portal, num_links=1):
        """
        Obtém os links dos CSVs mais recentes do portal da transparência.

        Parameters:
        - url_portal (str): URL do portal da transparência.
        - num_links (int): Número de links desejados.

        Returns:
        - list or str: Lista de hrefs ou mensagem de erro.
        """

        response = requests.get(url_portal, verify=False)
        
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
                self.print_api("Menos de 3 links encontrados com o padrão desejado.")
                return None
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

    def ler_csv_e_transformar_em_servidores(self, url):
        """
        Lê o conteúdo do CSV e o transforma em objetos ServidorCSV.

        Parameters:
        - url (str): URL do CSV.

        Returns:
        - list or None: Lista de servidores ou None em caso de erro.
        """
        try:

            # Faz a requisição e obtém o conteúdo do CSV
            response = request.urlopen(url)
            conteudo_csv = response.read().decode('utf-8')

            # Usa o módulo StringIO para transformar a string em um objeto "file-like"
            arquivo_csv = StringIO(conteudo_csv)

            # Utiliza o módulo csv para ler o arquivo CSV
            leitor_csv = csv.reader(arquivo_csv)
            
            # Pula o cabeçalho se houver
            next(leitor_csv, None)

            # Inicializa uma lista para armazenar os objetos
            lista_de_servidores = []

            # Itera sobre as linhas do CSV e cria objetos para cada linha
            for linha in leitor_csv:
                # Adapte isso conforme a posição das colunas no seu CSV
                orgao = linha[1]
                nome = linha[4]
                valor = float(linha[19].replace(",", "."))  # Assume que o valor é uma representação numérica
                vantagem_desconto = linha[17]

                # Se a coluna "VantagemDesconto" contiver o valor "V", cria o objeto e adiciona à lista
                if vantagem_desconto.lower() == "v":
                    servidor = ServidorCSV(orgao, nome, valor)
                    lista_de_servidores.append(servidor)

            return lista_de_servidores

        except Exception as e:
            self.print_api(f"Erro ao ler o CSV: {e}")
            return None
        
    def filtrar_e_agrupar_via_api_servidores_por_email(self,email, guidString):
        """
        Consulta a API do dominio e retorna uma lista de ServidorCSV.

        Parameters:
        - email (str): Endereço de email pertencente ao domínio es.gov.br.

        Returns:
        - lista de ServidorCSV: Uma lista de objetos ServidorCSV preenchida ou caso não encontre retornará None.  
        """
        match = re.match(r'^([^@]+)@([^@]+)$', email)
        if match:
            nome_usuario = match.group(1)
            orgao = match.group(2).split('.')[0]  # Assume que o domínio é a primeira parte do domínio do email

            if match.group(2) == self.dominio: # Se o email não tem subdomínio que identifique o orgão de origem o filtro por órgão será desconsiderado na consulta
                orgao = ''
            
            # Construa a URL completa
            url_completa = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_SQL.format(guid=guidString, nome=nome_usuario.split('.')[0], sobrenome=nome_usuario.split('.')[1] if '.' in nome_usuario else '', orgao=orgao)

            # Faça a chamada à API
            response = requests.get(url_completa, verify=False)

            # Verifique se a solicitação foi bem-sucedida (código de status 200)
            if response.status_code == 200:
                dados_api = response.json()

                # Inicializa um dicionário para armazenar os resultados agrupados
                resultados_agrupados = {}
                
                # Itera sobre cada registro no JSON retornado pela API
                for registro in dados_api["result"]["records"]:
                    # Aplica o filtro para remover linhas de 13º salário e férias
                    if not ("DECIMO TERCEIRO" in registro["Rubrica"] or "13" in registro["Rubrica"] or " FER" in registro["Rubrica"]):
                        chave = (registro["Orgao"], registro["Nome"])

                        # Converte o valor para float
                        valor = float(registro["Valor"].replace(",", "."))

                        # Atualiza os resultados agrupados
                        if chave in resultados_agrupados:
                            if registro["VantagemDesconto"] == "V":
                                resultados_agrupados[chave] += valor                                                            
                        else:
                            if registro["VantagemDesconto"] == "V":
                                resultados_agrupados[chave] = valor                                                            
                
                # Adiciona 1/12 (Decimo Terceiro) e 1/3/12 (percentual Férias para um mês) do valor ao resultado durante a criação da lista servidores_agrupados
                servidores_agrupados = [ServidorCSV(orgao, nome, valor + valor/12 + valor/3/12) for (orgao, nome), valor in resultados_agrupados.items()]

                return servidores_agrupados
            else:
                # Se a solicitação não for bem-sucedida, trate o erro conforme necessário
                self.print_api(f"Falha na solicitação à API. Código de status: {response.status_code}")
                return None

    def filtrar_e_agrupar_servidores_por_email(self,email, lista_de_servidores):
        """
        Filtra servidores com base no email e agrupa por orgão e nome.

        Parameters:
        - email (str): Email para filtrar os servidores.
        - lista_de_servidores (list): Lista de servidores para filtrar.
        
        Returns:
        - dict or None: Dicionário agrupado por orgão e nome ou None em caso de erro.
        """
        # Extrair nome de usuário e domínio do email
        match = re.match(r'^([^@]+)@([^@]+)$', email)
        if match:
            nome_usuario = match.group(1)
            orgao = match.group(2).split('.')[0]  # Assume que o domínio é a primeira parte do domínio do email
            
            if match.group(2) == self.dominio: # Se o email não tem subdomínio que identifique o orgão de origem o filtro por órgão será desconsiderado na consulta
                orgao = ''

            # Filtrar servidores com base no nome de usuário e orgao
            servidores_filtrados = [
                servidor for servidor in lista_de_servidores
                if all(part.lower() in servidor.nome.lower() for part in nome_usuario.lower().split('.')) and servidor.orgao.lower() == orgao.lower()
            ]

            # Agrupar servidores por orgao e nome
            resultados_agrupados = {}
            for servidor in servidores_filtrados:
                chave = (servidor.orgao, servidor.nome)
                if chave in resultados_agrupados:
                    resultados_agrupados[chave].append(servidor)
                else:
                    resultados_agrupados[chave] = [servidor]

            return resultados_agrupados
        else:
            self.print_api("Formato de email inválido.")
            return None


# Exemplo de utilização
if __name__ == "__main__":
    api = Api()
    servidor = api.get_remuneracao_por_tipo_origem("fulano.tal@iema.es.gov.br", False)
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")
    servidor = api.get_remuneracao_por_tipo_origem("ciclano.beltrano@iema.es.gov.br", False)
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")

     
