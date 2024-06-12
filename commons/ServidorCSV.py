import json

class ServidorCSV:
    """
    Classe para representar um servidor público a partir dos dados do CSV.
    """
    def __init__(self, orgao, nome, valor):
        self.orgao = orgao
        self.nome = nome
        self.valor = valor
    
    def to_json(self):
        if isinstance(self, ServidorCSV):
            return {
                'orgao': self.orgao,
                'nome': self.nome,
                'valor': self.valor,
                'email': self.email
            }
        raise TypeError("Objeto ServidorCSV não é serializável!")
