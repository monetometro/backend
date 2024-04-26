import json


class ServidorModel:
    """
    Classe para representar um servidor público a partir dos dados da fonte.
    """
    def __init__(self, orgao, nome, valor, dominio=None):
        self.orgao = orgao
        self.nome = nome
        self.valor = valor
        self.tokens = nome.split()        
        if dominio != None:
            self.dominio = dominio
            self.tokens.append(dominio)
    
    def to_json(self):
        if isinstance(self, ServidorModel):
            return {
                'orgao': self.orgao,
                'nome': self.nome,
                'valor': self.valor,
                'email': self.email
            }
        raise TypeError("Objeto ServidorModel não é serializável!")
    
   
    @staticmethod
    def to_json_list(servidores):
        if all(isinstance(servidor, ServidorModel) for servidor in servidores):
            return [servidor.to_json() for servidor in servidores]
        raise TypeError("Lista de ServidorModel contém objetos não serializáveis!")
