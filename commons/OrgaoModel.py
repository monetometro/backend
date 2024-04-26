class OrgaoModel:
    def __init__(self, nome, dominio, sigla, tld):
        self.nome = nome
        self.sigla = sigla
        self.dominio = dominio
        self.tld = tld

    def to_json(self):
        if isinstance(self, OrgaoModel):
            return {
                'nome': self.nome,
                'dominio': self.dominio,
                'sigla': self.sigla,
                'tld' : self.tld
            }
        raise TypeError("Objeto OrgaoModel não é serializável!")
