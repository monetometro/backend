"""
Este script realiza operações em dados de servidores públicos obtidos do portal da dados abertos do estado do Espírito Santo.
O objetivo geral é extrair o rendimento total de um servidor do tribunal de contas do ES. 
O Tribunal disponibiliza um link estático que fornece os dados mais atualizados de remuneração dos seus servidores.
    1 - Considerando que a URL é estática não necessita lógica para identificar o link mais recente;
    2 - Buscar, via filtro por consulta API, os objetos ServidorCSV por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [filtrar_e_agrupar_via_api_servidores_por_email]    

Autor: https://github.com/stgustavo
Data: 2023-01-01

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
PATH_PORTAL_SQL = '/api/3/action/datastore_search?q={nome}%{sobrenome}&resource_id={guid}'
GUID_DATASOURCE = 'f07af7e6-80f1-4726-b938-632123dfe30e'

class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores Tribunal de Contas do Estado do Espírito Santo.
    
    Domínio implementado: 
    - tcees.tc.br   

    """   
    def __init__(self):
        super().__init__("tcees.tc.br",URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_SQL.format(guid=GUID_DATASOURCE, nome='', sobrenome=''))

    def get_remuneracao(self, email):
        servidores = self.filtrar_e_agrupar_via_api_servidores_por_email(email, GUID_DATASOURCE)
        servidor = ServidorCSV("TCEES", nome=servidores[0].nome, valor=servidores[0].valor) if servidores else None

        if servidor == None:
            self.print_api("Nenhum servidor encontrado com base no email.")
        else:
            self.print_api(f"Orgao: TCEES, Nome: {servidor.nome}, Remuneração: {servidor.valor}")                 
            servidor.email = email    
            return servidor            
        
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
            
            # Construa a URL completa
            url_completa = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_SQL.format(guid=guidString, nome=nome_usuario.split('.')[0], sobrenome=nome_usuario.split('.')[1] if '.' in nome_usuario else '')

            # Faça a chamada à API
            response = requests.get(url_completa, verify=False)

            # Verifique se a solicitação foi bem-sucedida (código de status 200)
            if response.status_code == 200:
                dados_api = response.json()

                # Inicializa um dicionário para armazenar os resultados agrupados
                resultados_agrupados = {}
                maior_competencia = max((registro["Competencia"] for registro in dados_api["result"]["records"]), default=None)

                # Itera sobre cada registro no JSON retornado pela API
                for registro in dados_api["result"]["records"]:
                    # Aplica o filtro para remover linhas de 13º salário e férias
                    if maior_competencia in registro["Competencia"] and not ("FERIAS" in registro["DescricaoEvento"] or "13" in registro["DescricaoEvento"] or " FÉRIAS" in registro["DescricaoEvento"]):
                        chave = (registro["Nome"])

                        # Converte o valor para float
                        valor = float(registro["Valor"].replace(",", "."))

                        # Atualiza os resultados agrupados
                        if chave in resultados_agrupados:
                            if registro["TipoEvento"] == "C":
                                resultados_agrupados[chave] += valor                                
                        else:
                            if registro["TipoEvento"] == "C":
                                resultados_agrupados[chave] = valor   
                    
                
                # Adiciona 1/12 (Decimo Terceiro) e 1/3/12 (percentual Férias para um mês) do valor ao resultado durante a criação da lista servidores_agrupados
                servidores_agrupados = [ServidorCSV("TCEES", nome, valor + valor/12 + valor/3/12) for (nome), valor in resultados_agrupados.items()]

                return servidores_agrupados
            else:
                # Se a solicitação não for bem-sucedida, trate o erro conforme necessário
                self.print_api(f"Falha na solicitação à API. Código de status: {response.status_code}")
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

     
