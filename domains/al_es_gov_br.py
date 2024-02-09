"""
Este script realiza operações em dados de servidores públicos obtidos do portal da dados abertos do estado do Espírito Santo.
O objetivo geral é extrair o rendimento total de um servidor da Assembleia Legislativa do ES. 
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
import json
from urllib import request
from io import StringIO
from commons.ServidorCSV import ServidorCSV
from commons.AbstractETL import AbstractETL 
from unidecode import unidecode


# Constantes
URL_PORTAL_TRANSPARENCIA = "https://www.al.es.gov.br/Transparencia/ListagemServidoresTable"
URL_CONSULTA_VINCULOS = "https://www.al.es.gov.br/Transparencia/ListagemServidoresVinculosData?id={matricula}"
URL_CONSULTA_SALARIOS ="https://www.al.es.gov.br/Transparencia/ServidorDetalhes/?matricula={matricula}"

class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores da Assembleia Legislativa do Estado do Espírito Santo.
    
    Domínio implementado: 
    - al.es.gov.br   

    """   
    def __init__(self):
        super().__init__("al.es.gov.br",URL_PORTAL_TRANSPARENCIA)

    def get_remuneracao(self, email):
        # 1) Descobrir a matricula do servidor e vínculo mais recente
        lista_matriculas = self.extrair_json_pagina(URL_PORTAL_TRANSPARENCIA)
                
        lista_servidores_filtro = self.filtrar_e_agrupar_servidores_por_email(email, lista_matriculas)
        servidor_ales = lista_servidores_filtro[0] if lista_servidores_filtro else None

        matricula_vinculo = self.obter_vinculo_mais_recente(servidor_ales.get('Matricula'))
        # 3) extrair json do HTML que contem os dados de salário. extrair_json_do_html(matricula)
        json = self.extrair_json_pagina(URL_CONSULTA_SALARIOS.format(matricula=matricula_vinculo),False)
        # 4) extrair último salário. extrair_salario_base(json_data)
        salario = self.extrair_remuneracao_media_mensal(json)

        servidor = ServidorCSV("ALES", nome=servidor_ales.get("Nome"), valor=salario)
        if servidor != None:
            self.print_api(f"Orgao: {servidor.orgao}, Nome: {servidor.nome}, Remuneração: {servidor.valor}")
            servidor.email = email
            return servidor
        else:
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

            # Filtrar servidores com base no nome de usuário e sobrenome
            servidores_filtrados = [
                servidor for servidor in lista_de_servidores
                if unidecode(nome_usuario.split('.')[0].lower()) in unidecode(servidor.get("Nome").lower())
                and unidecode(nome_usuario.split('.')[1].lower()) in unidecode(servidor.get("Nome").lower())
            ]            
            return servidores_filtrados
        else:
            self.print_api("Formato de email inválido.")
            return None
    
    def organizar_json_string(self, string):
        # Encontrar todos os objetos JSON na string
        matches = re.findall(r'\{.*?\}', string)

        # Criar uma lista com os objetos JSON
        lista_json = []

        for match in matches:
            # Adicionar colchete de fechamento ausente, se necessário
            match_corrigido = match + ']' if match.count('[') > match.count(']') else match
            match_corrigido = match_corrigido + '}' if match_corrigido.count('{') > match_corrigido.count('}') else match_corrigido

            try:
                json_data = json.loads(match_corrigido)
                lista_json.append(json_data)
            except json.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON: {e}")
        
        return lista_json

    # Função para extrair o JSON de um script HTML
    def extrair_json_do_html(self,matricula):

        # Fazer a requisição para obter o conteúdo da página
        response = requests.get(URL_CONSULTA_SALARIOS.format(matricula=matricula), verify=False)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        script_tag = soup.find('script')

        if script_tag:
            # Extrair o conteúdo do script
            script_content = script_tag.contents[0]
            return script_content

        return None

    def extrair_remuneracao_media_mensal(self, json_data):
        try:

            lista_salario_base  = next((item for item in json_data if item.get('NomeCampo') == 'ValorSalarioBase'), None)
            tupla_mes_salario_base = next(((f'Valor{i}', lista_salario_base[f'Valor{i}']) for i in range(12, 0, -1) if lista_salario_base.get(f'Valor{i}') is not None), None)
            atributo_mes, valor_salario_base = tupla_mes_salario_base

            lista_outras_remuneracoes  = next((item for item in json_data if item.get('NomeCampo') == 'ValorOutrasRemuneracoes'), None)

            decimo_terceiro = [item for item in json_data if any(item.get(f'Valor{i}', 0.0) > 0 for i in range(1, 13)) and '13' in item.get('Nome', '') and 'SALARIO ANUAL' in item.get('Nome', '')]
            aux_alimentacao = [item for item in lista_outras_remuneracoes.get("SubVerbaList") if any(item.get(f'Valor{i}', 0.0) > 0 for i in range(1, 13)) and 'AUX' in item.get('Nome', '') and 'ALIMENTA' in item.get('Nome', '')]
            valor_auxilio_alimentacao = 0
            achou_registro_13_salario = False
            if aux_alimentacao != []:
                valor_auxilio_alimentacao = aux_alimentacao[0].get(atributo_mes)
            if decimo_terceiro != []:
                achou_registro_13_salario = True
                valor_auxilio_alimentacao = valor_auxilio_alimentacao/2
            
            remuneracao_media_mensal = valor_salario_base
            if lista_outras_remuneracoes != None:
                remuneracao_media_mensal += lista_outras_remuneracoes.get(atributo_mes)
            if achou_registro_13_salario :
                remuneracao_media_mensal -= valor_auxilio_alimentacao
                remuneracao_media_mensal -= valor_salario_base
            
            # Adciono 1/3 férias, 13 Salario e Aux. Alimentacao do 13 salario 
            remuneracao_media_mensal += (valor_salario_base/12/3 + valor_salario_base/12 + valor_auxilio_alimentacao/12)
                    
            return remuneracao_media_mensal

        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON: {e}")
        
        return None


    def extrair_json_pagina(self, url, foundByArray=True):
        try:
            # Fazer a requisição para obter o conteúdo da página
            response = requests.get(url, verify=False)
            response.raise_for_status()

            # Usar uma expressão regular para encontrar padrões JSON
            pattern = re.compile(r'"data":\s*\[({.*?})\]', re.DOTALL)

            # Encontrar todas as correspondências na string
            matches = pattern.findall(response.text)

            # Processar cada correspondência encontrada
            for match in matches:
                try:
                    # Carregar a string JSON da correspondência
                    json_data_list = json.loads(f'[{match}]') if foundByArray else self.organizar_json_string(match)
                    return json_data_list

                except json.JSONDecodeError as e:
                    print(f"Erro ao decodificar JSON: {e}")

        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer a requisição: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Erro ao decodificar JSON: {e}")
            return None

    def obter_vinculo_mais_recente(self, matricula):
        try:
            # Fazer a requisição para obter o conteúdo JSON da URL
            response = requests.get(URL_CONSULTA_VINCULOS.format(matricula=matricula), verify=False)
            response.raise_for_status()

            # Converter o conteúdo JSON para uma lista de dicionários
            dados = response.json()

            # Procurar o registro com "DataDemissao" igual a null
            for registro in dados:
                if registro.get("DataDemissao") is None:
                    return registro.get("CodigoCadfu")

            # Se nenhum registro corresponder, retornar None
            return None

        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer a requisição: {e}")
            return None
        except ValueError as e:
            print(f"Erro ao decodificar JSON: {e}")
            return None
