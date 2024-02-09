"""
Este script realiza operações em dados de servidores públicos obtidos do portal da transparência do Poder Judiciário do Espírito Santo - TJES.
A estratégia utilizada para arquivos ODS consiste em:
    1 - Identificar o arquivo mensal de remunerações mais recente; [obter_links_arquivo_mais_recentes]
    2 - Extrair todos os registros de remuneração transformando-os em objetos ServidorCSV; [ler_arquivo_e_transformar_em_servidores]
    3 - Buscar na lista de ServidorCSV por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [filtrar_e_agrupar_servidores_por_email]    
        
Autor: https://github.com/stgustavo
Data: 2023-12-10

"""

import requests
from bs4 import BeautifulSoup
import re
import os
from urllib import request
import ezodf
import io
from io import StringIO
import pandas as pd
import csv
from commons.ServidorCSV import ServidorCSV
from commons.AbstractETL import AbstractETL 
from unidecode import unidecode

# Constantes
URL_PORTAL_TRANSPARENCIA = "http://www.tjes.jus.br"
PATH_PORTAL_REMUNERACOES = "/portal-da-transparencia/pessoal/folha-de-pagamento"
PATH_PORTAL_ODS = ["/wp-content/uploads/{}"]

class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores do Pode Judiciário do Espírito Santo.
    
    Domínio implementado: 
    - tjes.jus.br   

    """   
    def __init__(self):
        super().__init__("tjes.jus.br",URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_REMUNERACOES)
        # Crio uma variável para armazenar os dados em memória para otimizar a busca por mais de um email usando a mesma instancia da classe
        self.database = {'arquivo': None, 'servidores': []}

    def get_remuneracao(self, email):
        servidor= self.get_remuneracao_tjes(email)            
        if servidor == None:
            self.print_api("Nenhum servidor encontrado com base no email.")
        else:
            servidor.email = email    
            return servidor
            

    def get_remuneracao_tjes(self, email):
        """
        Retorna um objeto ServidorCSV do portal da transparência baseado no email recebido.

        Parameters:
        - email (str): Endereço de email pertencente ao domínio tjes.jus.br.
        
        Returns:
        - ServidorCSV: Uma classe ServidorCSV preenchida ou caso não encontre retornará None.  
        """
        arquivos_mais_recentes = self.obter_links_arquivo_mais_recentes(URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_REMUNERACOES)
        if isinstance(arquivos_mais_recentes, list):
            
            # Inicializa um dicionário para armazenar os resultados agrupados
            resultados_agrupados = {}
            
            servidores = self.get_database_by_link(arquivos_mais_recentes) or []

            # Itera sobre cada arquivo mais recente e executa a rotina
            if (servidores==[]):
                for arquivo in arquivos_mais_recentes:                            
                    for path in PATH_PORTAL_ODS:
                        servidores.extend(self.ler_arquivo_e_transformar_em_servidores(URL_PORTAL_TRANSPARENCIA + path.format(arquivo)))
            
                self.add_to_database(arquivos_mais_recentes, servidores)
            
            filtrados = self.filtrar_e_agrupar_servidores_por_email(email, servidores)

            # Dicionário para armazenar a soma total para cada combinação única de Nome e Orgao
            resultados_agrupados = {}

            # Itera sobre os itens filtrados
            for item in filtrados:
                chave = (item.nome, item.orgao)

                # Adiciona o Valor ao total existente ou 0 se a chave não existir ainda
                resultados_agrupados[chave] = resultados_agrupados.get(chave, 0) + item.valor

            if resultados_agrupados:
                for chave, valor in resultados_agrupados.items():
                    nome, orgao = chave
                    servidor = ServidorCSV(orgao=orgao, nome=nome, valor=valor)
                    if servidor != None:
                        self.print_api(f"Orgao: {servidor.orgao}, Nome: {servidor.nome}, Remuneração: {servidor.valor}")
                        servidor.email = email
                    return servidor
            else:
                return None
        else:
            return None

    def obter_links_arquivo_mais_recentes(self, url_portal, num_links=2):
        """
        Obtém os links dos CSVs mais recentes do portal da transparência.

        Parameters:
        - url_portal (str): URL do portal da transparência.
        - num_links (int): Número de links desejados.

        Returns:
        - list or str: Lista de hrefs ou mensagem de erro.
        """

        if self.database['arquivo']:
            return self.database['arquivo']

        response = requests.get(url_portal, verify=False)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Encontrar todos os links que correspondem ao padrão Remuneracoes-MM_YYYY.csv
            links_ods = [a['href'] for a in soup.find_all('a', href=re.compile(r'ANEXO-VIII_\d{6}_.*\.ods'))]
            links_ods.sort(key=lambda a: self.extrair_mes_ano_do_link(a), reverse=True)
            
            # Validar se existem pelo menos num_links antes de tentar retornar
            if len(links_ods) >= num_links:
                # Extrair os href relativos correspondentes aos títulos mais recentes
                hrefs_mais_recentes = [os.path.basename(link) for link in links_ods[:num_links]]
                self.database['arquivo'] = hrefs_mais_recentes
                return hrefs_mais_recentes
            else:
                self.print_api("Menos de 3 links encontrados com o padrão desejado.")
                return None
        else:
            self.print_api( f"Erro ao acessar a página. Status code: {response.status_code}")
            return None

    # Função para extrair a data do link
    def extrair_mes_ano_do_link(self,link):
        match = re.search(r'ANEXO-VIII_(\d{6})_', link)
        if match:
            return match.group(1)
    
    def ler_arquivo_e_transformar_em_servidores(self, url_ods, quantidade_linhas_header=8):
        """
        Lê o conteúdo do ODS e o transforma em objetos ServidorCSV.

        Parameters:
        - url (str): URL do ODS.

        Returns:
        - list or None: Lista de servidores ou None em caso de erro.
        """
        try:
            # Faz a requisição e obtém o conteúdo do arquivo ODS
            response = request.urlopen(url_ods)
            conteudo_ods = response.read()

            # Carrega o conteúdo ODS usando ezodf
            doc = ezodf.opendoc(io.BytesIO(conteudo_ods))

            # Inicializa uma lista para armazenar os servidores
            lista_de_servidores = []

            # Itera sobre as folhas do ODS (assumindo apenas uma folha)
            sheet =  doc.sheets[0]

            contador_linha_branco=0
            # Itera sobre as linhas da planilha
            for linha in sheet.rows() if sheet is not None else []:
                quantidade_linhas_header-=1
                # Pula o cabeçalho ({quantidade_linhas_header} linhas)
                if (quantidade_linhas_header) > 0 :
                    next(sheet.rows())
                else:
                    # Verifica se linha não é None antes de tentar iterar
                    valor_linha = [c.value for c in linha] if linha is not None else []
                    if valor_linha[2] is not None:
                        valor = float(str(valor_linha[11] if valor_linha[11] is not None else 0).replace(",", "."))  # Assume que o valor é uma representação numérica
                        servidor = ServidorCSV('TJES', valor_linha[2], valor)
                        lista_de_servidores.append(servidor)
                    else:
                        contador_linha_branco +=1
                    if contador_linha_branco >=3:
                        break

            return lista_de_servidores

        except Exception as e:
            self.print_api(f"Erro ao ler o CSV: {e}")
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
        # Extrair nome de usuário e domínio do email
        match = re.match(r'^([^@]+)@([^@]+)$', email)
        if match:
            nome_usuario = match.group(1)

            # Filtrar servidores com base no nome de usuário e sobrenome
            servidores_filtrados = [
                servidor for servidor in lista_de_servidores
                if unidecode(nome_usuario.split('.')[0].lower()) in unidecode(servidor.nome.lower())
                and unidecode(nome_usuario.split('.')[1].lower()) in unidecode(servidor.nome.lower())
            ]
            
            return servidores_filtrados
        else:
            self.print_api("Formato de email inválido.")
            return None
        

# Exemplo de utilização
if __name__ == "__main__":
    api = Api()
    servidor = api.get_remuneracao_por_tipo_origem("fulano.tal@tjes.jus.br", False)
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")
    servidor = api.get_remuneracao_por_tipo_origem("ciclano.beltrano@tjes.jus.br", False)
    if servidor == None:
        api.print_api("Nenhum servidor encontrado com base no email.")

     
