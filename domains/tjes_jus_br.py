"""from
Este script realiza operações em dados de servidores públicos obtidos do portal da transparência do Poder Judiciário do Espírito Santo - TJES.
A estratégia utilizada para arquivos ODS consiste em:
    1 - Identificar o arquivo mensal de remunerações mais recente; [obter_links_arquivo_mais_recentes]
    2 - Extrair todos os registros de remuneração transformando-os em um dataframe; [ler_arquivo_e_transformar_em_servidores]
    3 - Buscar na lista de servidores por um determinado email, para isso infere-se que a parte de login no email contenha 
        o nome e sobrenome do servidor e que a parte do domínio do email contenha a sigla do órgão de lotação; [self.filter_by_email_login(email)]    
        
Autor: https://github.com/stgustavo
Data: 2023-12-10

"""

from bs4 import BeautifulSoup
import re
import os
import ezodf
import io
import pandas as pd
from commons.AbstractETL import AbstractETL 

# Constantes
URL_PORTAL_TRANSPARENCIA = "http://www.tjes.jus.br"
PATH_PORTAL_REMUNERACOES = "/portal-transparencia/pessoal/folha-de-pagamento"
PATH_PORTAL_ODS = "/wp-content/uploads/{}"

class Api(AbstractETL):
    """
    Classe para retorno de remuneração de servidores do Pode Judiciário do Espírito Santo.
    
    Domínio implementado: 
    - tjes.jus.br   

    """   
    def __init__(self):
        super().__init__(dominio="tjes.jus.br",
                         unidade_federativa="Espírito Santo",
                    portal_remuneracoes_url=URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_REMUNERACOES,
                    fn_obter_link_mais_recente=self.obter_links_arquivo_mais_recentes,
                    fn_ler_fonte_de_dados_e_transformar_em_dataframe=self.ler_arquivo_e_transformar_em_servidores
                    )

    def get_remuneracao(self, email):
        return self.run(email)           

    
    def obter_links_arquivo_mais_recentes(self,num_links=2):
        """
        Obtém os links dos CSVs mais recentes do portal da transparência.

        Parameters:
        - url_portal (str): URL do portal da transparência.
        - num_links (int): Número de links desejados.

        Returns:
        - list or str: Lista de hrefs ou mensagem de erro.
        """

        url_portal = self.portal_remuneracoes_url
        response = self.http_client.get(url_portal)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Encontrar todos os links que correspondem ao padrão Remuneracoes-MM_YYYY.csv
            links_ods = [a['href'] for a in soup.find_all('a', href=re.compile(r'ANEXO-VIII_\d{6}_.*\.ods'))]
            links_ods.sort(key=lambda a: self.extrair_mes_ano_do_link(a), reverse=True)
            
            # Validar se existem pelo menos num_links antes de tentar retornar
            if len(links_ods) >= num_links:
                # Extrair os href relativos correspondentes aos títulos mais recentes
                hrefs_mais_recentes = [os.path.basename(link) for link in links_ods[:num_links]]
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
    
    def ler_arquivo_e_transformar_em_servidores(self, lista_fontes_de_dados):
        """
        Lê o conteúdo do ODS e o transforma em um Dataframe.

        Parameters:
        - url (str): URL do ODS.

        Returns:
        - list or None: Lista de servidores ou None em caso de erro.
        """
        try:
            servidores = pd.DataFrame()

            for arquivo in lista_fontes_de_dados:
                url_ods = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_ODS.format(arquivo)

                # Faz a requisição e obtém o conteúdo do arquivo ODS
                response = self.http_client.get(url_ods)
                conteudo_ods = response.content

                # Carrega o conteúdo ODS usando ezodf
                doc = ezodf.opendoc(io.BytesIO(conteudo_ods))

                # Itera sobre as folhas do ODS (assumindo apenas uma folha)
                sheet =  doc.sheets[0]

                dados_linhas = [col[:] for col in doc.sheets[2].columns()]
                df = pd.DataFrame({col[0].value: [c.value for c in col[1:]] for col in dados_linhas})
                df.replace({None: ''}, inplace=True)
                palavras_excluir = ["DECIMO TERCEIRO", "13", " FER"]
                df_filtrado = df[~df.iloc[:, 8].str.contains('|'.join(palavras_excluir))]
                resultado = df_filtrado[df_filtrado['TIPO'] == 'C'].groupby('NOME')['VALOR'].sum()
                resultado = resultado.reset_index()
                resultado['REMUNERACAO_MENSAL_MEDIA'] = resultado.apply(lambda row: row['VALOR'] + row['VALOR']/3/12 + row['VALOR']/12, axis=1)
                resultado['DOMINIO'] = self.dominio
                resultado['SIGLA'] = "TJES"
                resultado['ORGAO'] = "Tribunal de Justiça do Estado do Espírito Santo"
                resultado = resultado[['NOME', 'REMUNERACAO_MENSAL_MEDIA', 'ORGAO', 'SIGLA', 'DOMINIO']]

                if not servidores.empty:
                    servidores = pd.concat([servidores, resultado], ignore_index=True)
                else:
                    servidores = resultado
           
            return servidores

        except Exception as e:
            self.print_api(f"Erro ao ler o CSV", e)
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

     
