"""
Este script realiza operações em dados de servidores públicos obtidos do portal da dados abertos do estado do Espírito Santo.
O objetivo geral é extrair o rendimento total de um servidor da Assembleia Legislativa do ES. 
O Tribunal disponibiliza um link estático que fornece os dados mais atualizados de remuneração dos seus servidores.
    1 - Considerando que a URL é estática não necessita lógica para identificar o link mais recente;
    2 - Buscar, via filtro por consulta API, os objetos servidores por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [filtrar_e_agrupar_via_api_servidores_por_email]    

Autor: https://github.com/stgustavo
Data: 2023-01-01

"""

import requests
from bs4 import BeautifulSoup
import re
import os
import json
import pandas as pd
from commons.AbstractETL import AbstractETL 
from concurrent.futures import ThreadPoolExecutor

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
        super().__init__(dominio="al.es.gov.br",
                         unidade_federativa="Espírito Santo",
                    portal_remuneracoes_url=URL_PORTAL_TRANSPARENCIA,
                    fn_obter_link_mais_recente=self.obter_links_competencia_mais_recentes,
                    fn_ler_fonte_de_dados_e_transformar_em_dataframe=self.ler_dados_e_transformar_em_servidores
                    )


    def get_remuneracao(self, email):
        return self.run(email)  
    
    def obter_links_competencia_mais_recentes(self):
        # 1) Descobrir a matricula do servidor e vínculo mais recente
        lista_matriculas = self.extrair_json_pagina(self.portal_remuneracoes_url)
        
        ### Como não existe arquivo único para definir a competencia (mês) dos dados divulgados é feito uma consulta amostral para identificar o mês mais recente com dados disponibilizados
        ### Inicio Cálculo amostral de Competencia

        contagem_competencias = {}

        # Função para contar as ocorrências da competência
        def contar_competencia(matricula):
            competencia = self.get_competencia_mais_recente(self.extrair_json_pagina(URL_CONSULTA_SALARIOS.format(matricula=self.obter_vinculo_mais_recente(matricula)), False))
            contagem_competencias[competencia] = contagem_competencias.get(competencia, 0) + 1

        # Usar ThreadPoolExecutor para executar as iterações em paralelo
        with ThreadPoolExecutor() as executor:
            # Mapear as iterações para threads
            executor.map(contar_competencia, [matricula["Matricula"] for matricula in lista_matriculas[:5]])

        # Encontrar a competência mais frequente
        resultado = [max(contagem_competencias, key=contagem_competencias.get)]
        self.lista_matriculas = self.extrair_json_pagina(self.portal_remuneracoes_url)
        return resultado
        ### Fim Cálculo amostral de Competencia


    def ler_dados_e_transformar_em_servidores(self, lista_fontes_de_dados):
        try:
            competencia_mais_recente = lista_fontes_de_dados[0]
            lista_matriculas = self.lista_matriculas
            database_path = self.get_database_by_link(competencia_mais_recente)
            servidores = pd.DataFrame()

            if not os.path.exists(database_path):                
                for servidor_ales in lista_matriculas:
                    matricula_vinculo = self.obter_vinculo_mais_recente(servidor_ales.get('Matricula'))
                    # 3) extrair json do HTML que contem os dados de salário. 
                    json = self.extrair_json_pagina(URL_CONSULTA_SALARIOS.format(matricula=matricula_vinculo),False)
                    if json != None:
                        # 4) extrair último salário. extrair_salario_base(json_data)
                        salario = self.extrair_remuneracao_media_mensal(json,competencia_mais_recente)

                        data = {"ORGAO":"Assembleia Legislativa do Estado do Espírito Santo", "NOME":servidor_ales.get("Nome"), "REMUNERACAO_MENSAL_MEDIA":salario, "SIGLA":"ALES", "DOMINIO":self.dominio}

                        if servidores.empty:
                            servidores = pd.DataFrame([data])
                        else:
                            servidores = pd.concat([servidores, pd.DataFrame([data])], ignore_index=True)

            return servidores
        except Exception as e:
            self.print_api(f"Erro ao ler dados", e)
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
        response = self.http_client.get(URL_CONSULTA_SALARIOS.format(matricula=matricula))
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        script_tag = soup.find('script')

        if script_tag:
            # Extrair o conteúdo do script
            script_content = script_tag.contents[0]
            return script_content

        return None
    
    def get_competencia_mais_recente(self, json_data):
        lista_salario_base  = next((item for item in json_data if item.get('NomeCampo') == 'ValorSalarioBase'), None)
        tupla_mes_salario_base = max(((chave, valor) for chave, valor in lista_salario_base.items() if chave.startswith("Valor") and isinstance(valor, (int, float)) and valor > 0), key=lambda x: x[0][-2:])
        atributo_mes, valor_salario_base = tupla_mes_salario_base
        return atributo_mes

    def extrair_remuneracao_media_mensal(self, json_data, competencia=None):
        try:
            lista_salario_base  = next((item for item in json_data if item.get('NomeCampo') == 'ValorSalarioBase'), None)
            if competencia == None:                
                tupla_mes_salario_base = max(((chave, valor) for chave, valor in lista_salario_base.items() if chave.startswith("Valor") and isinstance(valor, (int, float)) and valor > 0), key=lambda x: x[0][-2:])
                atributo_mes, valor_salario_base = tupla_mes_salario_base
            else:
                atributo_mes = competencia
                valor_salario_base = lista_salario_base.get(competencia)

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
            response = self.http_client.get(url)
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
            response = self.http_client.get(URL_CONSULTA_VINCULOS.format(matricula=matricula))
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
