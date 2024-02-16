class ApiInterface:
    """
    Classe com padrão de implementação para as classes de domínios habilitados pelo monetometro para obtenção de dados de remuneração de servidores.

    Methods:
    - get_remuneracao: Endereço de email pertencente ao domínio implementado.
        Parameters:
            - email (str): endereço de email pertencente ao domínio implementado
        Returns:
            - ServidorCSV: Uma classe ServidorCSV preenchida ou caso não encontre retornará None.  

    """
    def __init__(self, dominio):
        self.dominio = dominio

    def get_remuneracao(self, email):
        pass

    def print_api(self, msg):
        print("[" + self.dominio + "] "+msg)
