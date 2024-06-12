import os
import duckdb
import pandas as pd
from commons.utils import log,get_configuration_value,get_traceback_string
from commons.ServidorModel import ServidorModel
from commons.OrgaoModel import OrgaoModel
from commons.HTTPRequestManager import HTTPRequestManager
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import re
import json
from enum import Enum
from duckduckgo_search import DDGS
from datetime import datetime
from urllib.parse import urlparse
import threading

# Criar um bloqueio para controlar o acesso à seção crítica
lock = threading.Lock()


CACHE_DIRECTORY = get_configuration_value("CACHE_DIRECTORY")
CACHE_ORGAOS = os.path.join(CACHE_DIRECTORY, 'orgaos_db.json')

class AbstractETL:
    """
    Classe com padrão de implementação para as classes de domínios habilitados pelo monetometro para obtenção de dados de remuneração de servidores.

    Methods:
    - get_remuneracao: Obtém dados de remuneração de um servidor com base no endereço de e-mail.
        Parameters:
            - email (str): Endereço de e-mail pertencente ao domínio implementado.
        Returns:
            - ServidorModel: Uma instância da classe ServidorModel preenchida ou None se não encontrado.

    - print_api: Imprime uma mensagem formatada com o nome do domínio.
        Parameters:
            - msg (str): Mensagem a ser impressa.

    - add_to_database: Adiciona informações ao banco de dados global associado ao domínio.
        Parameters:
            - links (str or list of str): Links relacionados à remuneração dos servidores.
            - servidores (list): Lista de servidores a serem associados ao domínio no banco de dados.

    - get_database_by_link: Obtém a lista de servidores associada a um link específico no banco de dados global.
        Parameters:
            - link (str or list of str): Link ou lista de links para verificar no banco de dados.
        Returns:
            - list: Lista de servidores associada ao link, ou uma lista vazia se o link não estiver presente.
    """

    class SearchEngineEnum(Enum):
        BING=1
        DUCKDUCKGO=2
        GOOGLE=3


    def __init__(self, unidade_federativa, dominio, portal_remuneracoes_url, fn_obter_link_mais_recente, fn_ler_fonte_de_dados_e_transformar_em_dataframe):

        # Criar o cache de orgaos caso não exista
        if not os.path.isfile(CACHE_ORGAOS):
            with open(CACHE_ORGAOS, 'w') as f:                    
                f.write("[]")

         # Verifica se o parâmetro 'unidade_federativa' é uma string
        if not isinstance(unidade_federativa, str):
            raise TypeError("O parâmetro 'unidade_federativa' deve ser uma string.")
        
        # Verifica se o parâmetro 'dominio' é uma string
        if not isinstance(dominio, str):
            raise TypeError("O parâmetro 'dominio' deve ser uma string.")
        
        # Verifica se o parâmetro 'portal_remuneracoes_url' é uma string
        if not isinstance(portal_remuneracoes_url, str):
            raise TypeError("O parâmetro 'portal_remuneracoes_url' deve ser uma string.")
        
        # Verifica se 'fn_obter_link_mais_recente' é uma função
        if fn_obter_link_mais_recente is None or not callable(fn_obter_link_mais_recente):
            raise TypeError("O parâmetro 'fn_obter_link_mais_recente' deve ser uma função.")
        
        # Verifica se 'fn_ler_fonte_de_dados_e_transformar_em_dataframe' é uma função
        if fn_ler_fonte_de_dados_e_transformar_em_dataframe is None or not callable(fn_ler_fonte_de_dados_e_transformar_em_dataframe):
            raise TypeError("O parâmetro 'fn_ler_fonte_de_dados_e_transformar_em_dataframe' deve ser uma função.")

        self.dominio = dominio
        self.uf=unidade_federativa
        self.hash_arquivo = ''
        self.portal_remuneracoes_url = portal_remuneracoes_url
        self.fn_obter_link_mais_recente = fn_obter_link_mais_recente
        self.fn_ler_fonte_de_dados_e_transformar_em_dataframe = fn_ler_fonte_de_dados_e_transformar_em_dataframe
        self.http_client = HTTPRequestManager(verify_ssl=False)

    
    def _searching_web_scrapper(self, orgao, search_engine_enum):
        search_domain=""
        try:
            if search_engine_enum == self.SearchEngineEnum.BING:
                search_domain = "bing"
            elif search_engine_enum == self.SearchEngineEnum.DUCKDUCKGO:
                search_domain="duckduckgo"
                results = DDGS().text(f'{orgao.replace(" ", "%20")}%20{self.dominio}', max_results=5, region="pt-br")
                dominio = re.compile(r'https?://(?:www\.)?([a-zA-Z0-9.-]+)').search(results[0].get("href")).group(1)
                return {'dominio': dominio, 'path': results[0].get('href').split(dominio)[1]}

            else:
                search_domain = "google"

            url_pesquisa = f'https://www.{search_domain}.com/search?q={orgao.replace(" ", "%20")}%20{self.dominio}'

            # solicitação HTTP para obter o conteúdo da página
            response = self.http_client.get(url_pesquisa)
            response.raise_for_status()  # Levanta uma exceção se a solicitação for mal sucedida

            # BeautifulSoup para analisar o HTML da página
            soup = BeautifulSoup(response.text, 'html.parser')

            if search_engine_enum == self.SearchEngineEnum.BING:
                elementos_tptxt = soup.find_all(class_='tptxt')
                dominios = []

                for elemento in elementos_tptxt:
                    url = urlparse(elemento.find('cite').get_text())
                    dominios.append((re.sub(r'^[wW]{2,3}\d*\.', '', url.netloc), url.path))
                dominios_br = [{'dominio': dominio, 'path': path} for dominio, path in dominios if dominio.endswith('.br')]

            else:
                dominios =  [(re.sub(r'^[wW]{2,3}\d*\.', '', urlparse(a['href']).netloc), urlparse(a['href']).path) for a in soup.find_all('a', {'href': True}) if a.find('h3')]
                dominios_br = [{'dominio': item[0], 'path': item[1]} for item in dominios if item[0].endswith('.br')]

            # retorna o primeiro registro *.br retornado na pesquisa
            return dominios_br[0]

        except Exception as e:
            return f"Erro ao consultar o {search_domain}: {e}"
        

    def _get_databases(self):
        diretorio = CACHE_DIRECTORY
        extensao = ".db"

        # Listar todos os arquivos no diretório
        arquivos_no_diretorio = os.listdir(diretorio)

        # Filtrar apenas os arquivos com a extensão desejada
        arquivos_db = [arquivo for arquivo in arquivos_no_diretorio if arquivo.endswith(extensao) and "-" in arquivo]
        databases = {}
        for arquivo in arquivos_db:
            dominio = arquivo.split('-')[0]
            id = arquivo.split('-', 1)[1].split('.')[0]
            databases[dominio] = {"hash_arquivo": id.replace('-',''), "lista_servidores": os.path.join(diretorio, arquivo)}    
        
        return databases
    
    
    def _get_domain_db(self):
        '''
        Retorna o banco de servidores associado ao domínio corrente
        '''
        prefixo_arquivo = f'{self.dominio}-'

        # Listar todos os arquivos no diretório
        arquivos_no_diretorio = sorted(os.listdir(CACHE_DIRECTORY), key=lambda arquivo: os.path.getmtime(os.path.join(CACHE_DIRECTORY, arquivo)), reverse=True)

        caminho_arquivo=''

        # Iterar sobre os arquivos e excluir os indesejados
        for arquivo in arquivos_no_diretorio:
            if arquivo.startswith(prefixo_arquivo) and arquivo.endswith(".db"):
                caminho_arquivo = os.path.join(CACHE_DIRECTORY, arquivo)
                break
        
        return caminho_arquivo
    
    
    def _check_mandatory_columns(self, df_servidores):
        colunas_obrigatorias = ['ORGAO', 'NOME', 'REMUNERACAO_MENSAL_MEDIA', 'SIGLA', 'DOMINIO']

        colunas_presentes = set(map(str.lower, df_servidores))
        colunas_faltando = set(map(str.lower, colunas_obrigatorias)) - colunas_presentes

        if colunas_faltando:
            raise ValueError(f'Colunas obrigatórias ausentes: {colunas_faltando}')     
                 

    def get_remuneracao(self, email):
        """
        Obtém dados de remuneração de um servidor com base no endereço de e-mail.

        Parameters:
            - email (str): Endereço de e-mail pertencente ao domínio implementado.

        Returns:
            - ServidorModel: Uma lista de instâncias da classe ServidorModel preenchida ou None se não encontrado.
        """
        pass


    def run(self, email):
        guids_mais_recentes = self.fn_obter_link_mais_recente()
        if isinstance(guids_mais_recentes, list):
            database_path = self.get_database_by_link(guids_mais_recentes)
            servidores = pd.DataFrame()
            
            if not os.path.exists(database_path):                
                # Itera sobre cada guid mais recente e executa a rotina
                for guid in guids_mais_recentes:
                    if servidores.empty :
                        servidores = self.fn_ler_fonte_de_dados_e_transformar_em_dataframe([guid])
                    else:
                        servidores = pd.concat([servidores, self.fn_ler_fonte_de_dados_e_transformar_em_dataframe([guid])], ignore_index=True)
                self.add_to_database(guids_mais_recentes, servidores)   
            servidor = self.filter_by_email_login(email)
            for item in servidor:
                item.email = email    
            return servidor
        else:
            self.print_api("fn_obter_link_mais_recente precisa retornar uma lista []")
            return None
        

    def health_check(self): 
        """
        Verifica se a fonte de dados está operacional.

        Returns:
            - Boolean: True se operacional False se fora do ar.
        """
        #response = self.http_client.get(self.portal_remuneracoes_url, max_attempts=3)
        #portal_ativo = response.status_code == 200  
        domain_entry = self._get_databases().get(self.dominio, {})
        detalhes_db = self.get_file_metadata(domain_entry.get("lista_servidores")) 
        dias_atualizacao = (datetime.now() - detalhes_db.get("data")).days
        return dias_atualizacao <= 31 # and portal_ativo


    def get_file_metadata(self, nome_arquivo):
        try:
            # Obtém os metadados do arquivo
            metadados = os.stat(nome_arquivo)
            
            # Extrai informações específicas dos metadados
            nome = os.path.basename(nome_arquivo)
            tamanho = metadados.st_size
            data_modificacao = metadados.st_mtime

            data_modificacao_formatada = datetime.fromtimestamp(data_modificacao)

            # Retorna os metadados
            return {
                "nome": nome,
                "tamanho": tamanho,
                "data": data_modificacao_formatada
            }
        except FileNotFoundError:
            return "Arquivo não encontrado!"
        except Exception as e:
            return f"Ocorreu um erro ao obter os metadados do arquivo: {e}"
        

    def print_api(self, msg, exception = None):
        """
        Imprime uma mensagem formatada com o nome do domínio.

        Parameters:
            - msg (str): Mensagem a ser impressa.
        """
        if exception != None:
            print("[" + self.dominio + "] "+ msg + f" | ERROR: {exception} " + get_traceback_string())
        else:
            print("[" + self.dominio + "] "+msg)

    
    def log(self, msg):
        """
        Registra um log formatada com o nome do domínio.

        Parameters:
            - msg (str): Mensagem a ser registrada.
        """
        log("[" + self.dominio + "] "+msg)    


    def add_to_database(self, links, servidores):
        """
        Adiciona informações ao banco de dados global associado ao domínio.

        Parameters:
            - links (str or list of str): Links relacionados à remuneração dos servidores, serão os identificadores da fonte de dados mais recente.
            - servidores (list): Lista de servidores a serem associados ao domínio no banco de dados.
        """
        self._check_mandatory_columns(servidores)

        self.hash_arquivo = self.get_hash_from_links(links)

        prefixo_arquivo = f'{self.dominio}-'
        arquivo_atual = f'{self.hash_arquivo}.db'

        con = duckdb.connect(f'{CACHE_DIRECTORY}\\{prefixo_arquivo}{arquivo_atual}')
        try:
            con.register('lista_servidores', servidores)
            con.execute('DROP TABLE IF EXISTS servidores')
            con.execute('CREATE TABLE servidores AS SELECT *, ROW_NUMBER() OVER () AS _ID FROM lista_servidores WITH NO DATA')
            
            digest_result = con.execute("SELECT DOMINIO, COUNT(*) FROM servidores GROUP BY DOMINIO").fetchall()
            digest = {
                "uf": f"{self.uf}",
                "portal":f"{self.portal_remuneracoes_url}",
                "tld": f"{self.dominio}",
                "subs": [{"d": row[0], "c": row[1]} for row in digest_result]
            }

            # Salvando o digest no arquivo
            with open(f'{CACHE_DIRECTORY}\\{self.dominio}.digest', "w") as arquivo:
                json.dump(digest, arquivo)
        finally:    
            con.close()

        # Listar todos os arquivos no diretório
        arquivos_no_diretorio = os.listdir(CACHE_DIRECTORY)

        # Iterar sobre os arquivos e excluir os indesejados
        for arquivo in arquivos_no_diretorio:
            if arquivo.lower().startswith(prefixo_arquivo.lower()) and arquivo.lower().endswith(".db") and not arquivo.lower().endswith(arquivo_atual.lower()) and not arquivo.lower().endswith(".wal"):
                caminho_arquivo = os.path.join(CACHE_DIRECTORY, arquivo)
                os.remove(caminho_arquivo)
        
    
    def filter_by_email_login(self, email):
        '''
        Faz a filtragem dos objetos ServidorCSV que são aderentes ao email passado.

        Parameters:
            - email (str): Email do servidor para busca.

        Returns:
            - list of ServidorCSV: Lista de objetos ServidorCSV que atenderam ao critério de busca.
        '''

        caminho_arquivo = self._get_domain_db()
        dominio = email.split("@")[1]
        nome, sobrenome = email.split("@")[0].split(".")

        con = duckdb.connect(caminho_arquivo)
        try:

            query = "SELECT DISTINCT ORGAO, NOME, REMUNERACAO_MENSAL_MEDIA, DOMINIO, SIGLA FROM servidores WHERE (LOWER(NOME) like ? OR LOWER(REPLACE(NOME,' ','')) like ?) AND (concat(SIGLA,'.',DOMINIO) LIKE ? OR DOMINIO LIKE ?)"
            params = [f'%{nome}%{sobrenome}%', f'{nome}%{sobrenome}%', f'%{dominio}', f'%{dominio}']

            # Executar a consulta
            cursor = con.execute(query, params)

            # Lista para armazenar objetos ServidorModel
            lista_servidores = []

            # Obter todas as linhas retornadas
            rows = cursor.fetchall()

            # Iterar sobre os resultados e criar instâncias da classe ServidorModel
            for row in rows:
                orgao, nome, valor, dominio, sigla = row
                servidor = ServidorModel(orgao, nome, valor, sigla+'.'+dominio if sigla not in dominio else dominio)
                lista_servidores.append(servidor)

        finally:
            con.close()
        
        return lista_servidores
    

    def get_subdomains(self):
        try:
            with open(f'{CACHE_DIRECTORY}\\{self.dominio}.digest', "r") as arquivo:
                digest = json.load(arquivo)

            digest["refreshed"] = self.health_check() 
            return digest
        except Exception as e:
            self.log(f"Ocorreu um erro ao listar os subdomínios: {e}")
            return {}

    
    def get_database_by_link(self, links):
        """
        Obtém o banco de servidores associado ao link passado.

        Parameters:
            - links (str or list of str): Link ou lista de links para identificação do banco de servidores.

        Returns:
            - str: Endereço do banco de servidores, ou string vazia se não existir.
        """
        # Obtém a entrada associada ao domínio, retorna False se não existir
        domain_entry = self._get_databases().get(self.dominio, {})

        # Verifica se exite hash_arquivo
        link_presente = domain_entry.get("hash_arquivo", '').strip() == self.get_hash_from_links(links).strip() 
        
        # Retorna a lista de servidores se link_presente for True, do contrário, retorna uma lista vazia
        return domain_entry.get("lista_servidores") if link_presente else ''
    

    def get_hash_from_links(self, links):
        '''
        Gera o identificador único da base de dados corrente do domínio baseado nos links

        Parameters:
            - links (str or list of str): Link ou lista de links para verificar no banco de dados.
        
        Returns:
            - str: Hash gerado

        '''
        if isinstance(links, str):
            # Se 'links' for uma string, converte para lista para ordenar
            links = [links]
        elif not isinstance(links, list):
            raise ValueError("O argumento 'links' deve ser uma string ou uma lista de strings.")

        # Ordena a lista de links ascendentemente
        links = sorted(links)

        string_concatenada = '_'.join(map(lambda x: '_'.join(map(str, x)), links)).lower()

        hash_resultado = str(hashlib.sha256(string_concatenada.encode()).hexdigest())

        return hash_resultado   
    

    def get_cache_domains(self, orgaos=None):
        '''
        Durante as extrações, recebe a lista de órgãos encontrados nos arquivos CSV associa com os respectivos domínios, salva em cache o resultado retornando todos os valores disponíveis
        
        Parameters:
            - orgaos (list of str): Lista dos nomes de órgãos para verificar associação com os domínios
        
        Returns:
            - list of OrgaoModel: Lista com os órgãos e seus domínios
        '''        

        try:
            # Adquirir o bloqueio antes de entrar na seção crítica
            lock.acquire()

            # Lê o conteúdo do arquivo existente
            try:
                with open(CACHE_ORGAOS, 'r', encoding='utf-8') as arquivo:
                    conteudo_existente = json.load(arquivo)
            except json.JSONDecodeError:
                self.print_api(f"Erro ao desserializar o arquivo '{CACHE_ORGAOS}'. O conteúdo do arquivo pode estar corrompido ou em um formato inválido.")
                conteudo_existente = []

            # Lista para armazenar objetos OrgaoCSV
            lista_orgaos = []

            # Itera sobre cada dicionário na lista e cria objetos OrgaoCSV
            for item in conteudo_existente:
                orgao = OrgaoModel(item['nome'], item['dominio'], item['sigla'], item['tld'])
                lista_orgaos.append(orgao)
            
            if orgaos is not None:
                orgaos_nao_presentes = [orgao for orgao in orgaos if (orgao, self.dominio) not in [(item.nome, item.tld) for item in lista_orgaos]]
                for orgao in orgaos_nao_presentes:
                    orgao_normatizado = orgao.replace('.','. ').replace('"',' ')
                    resultado = self._searching_web_scrapper(orgao_normatizado, self.SearchEngineEnum.BING)
                    if 'dominio' not in resultado:
                        resultado = self._searching_web_scrapper(orgao_normatizado, self.SearchEngineEnum.GOOGLE)
                    if 'dominio' not in resultado:
                        resultado = self._searching_web_scrapper(orgao_normatizado, self.SearchEngineEnum.DUCKDUCKGO)
                    if 'dominio' in resultado:
                        lista_orgaos.append(OrgaoModel(orgao, resultado.get("dominio"), resultado.get("dominio").replace(self.dominio,'').split('.')[0], self.dominio))
                    else:
                        with open(CACHE_ORGAOS, 'w') as arquivo:
                            json.dump(lista_orgaos, arquivo, indent=1, default=lambda obj: obj.to_json())

                with open(CACHE_ORGAOS, 'w') as arquivo:
                    json.dump(lista_orgaos, arquivo, indent=1, default=lambda obj: obj.to_json())

            return [orgao for orgao in lista_orgaos if orgao.tld == self.dominio]            
        finally:
            # Liberar o bloqueio ao sair da função
            lock.release()