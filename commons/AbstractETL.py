import requests

VERIFY_SSL = False

class AbstractETL:
    """
    Classe com padrão de implementação para as classes de domínios habilitados pelo monetometro para obtenção de dados de remuneração de servidores.

    Methods:
    - get_remuneracao: Obtém dados de remuneração de um servidor com base no endereço de e-mail.
        Parameters:
            - email (str): Endereço de e-mail pertencente ao domínio implementado.
        Returns:
            - ServidorCSV: Uma instância da classe ServidorCSV preenchida ou None se não encontrado.

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

    database = {}
    
    def __init__(self, dominio, health_check_url):
        self.dominio = dominio
        self.links_remuneracao = []
        self.lista_servidores = []
        self.health_check_url = health_check_url

    def get_remuneracao(self, email):
        """
        Obtém dados de remuneração de um servidor com base no endereço de e-mail.

        Parameters:
            - email (str): Endereço de e-mail pertencente ao domínio implementado.

        Returns:
            - ServidorCSV: Uma instância da classe ServidorCSV preenchida ou None se não encontrado.
        """
        pass

    def health_check(self):
        """
        Verifica se a fonte de dados está operacional.

        Parameters:
            - url (str): Endereço da fonte de dados.

        Returns:
            - Boolean: True se operacional False se fora do ar.
        """
        response = requests.get(self.health_check_url, verify=VERIFY_SSL)
        return response.status_code == 200            


    def print_api(self, msg):
        """
        Imprime uma mensagem formatada com o nome do domínio.

        Parameters:
            - msg (str): Mensagem a ser impressa.
        """
        print("[" + self.dominio + "] "+msg)

    def add_to_database(self, links, servidores):
        """
        Adiciona informações ao banco de dados global associado ao domínio.

        Parameters:
            - links (str or list of str): Links relacionados à remuneração dos servidores.
            - servidores (list): Lista de servidores a serem associados ao domínio no banco de dados.
        """
        if isinstance(links, str):
            # Se 'links' for uma string, converte para lista para ordenar
            link = [links]
        elif not isinstance(links, list):
            raise ValueError("O argumento 'links' deve ser uma string ou uma lista de strings.")

        # Ordena a lista de links ascendentemente
        links_ordenado = sorted(links)

        # Concatena os links ordenados em uma única string
        self.links_remuneracao = links_ordenado
        self.print_api("Arquivos mais recentes: "+ "|".join(links_ordenado))

        # Atualiza a variável global
        AbstractETL.database[self.dominio] = {
            "links_remuneracao": self.links_remuneracao,
            "lista_servidores": servidores
        }

    def get_database_by_link(self, link):
        """
        Obtém a lista de servidores associada a um link específico no banco de dados global.

        Parameters:
            - link (str or list of str): Link ou lista de links para verificar no banco de dados.

        Returns:
            - list: Lista de servidores associada ao link, ou uma lista vazia se o link não estiver presente.
        """
        # Obtém a entrada associada ao domínio, retorna False se não existir
        domain_entry = AbstractETL.database.get(self.dominio, {})
        
        # Verifica se pelo menos um item do array link está presente em links_remuneracao
        link_presente = any(item in domain_entry.get("links_remuneracao", []) for item in link)

        # Retorna a lista de servidores se link_presente for True, do contrário, retorna uma lista vazia
        return domain_entry.get("lista_servidores") if link_presente else []
