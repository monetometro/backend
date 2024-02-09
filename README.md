# Guia para Desenvolver uma Classe de Extração de Dados de Remuneração de Servidores Públicos

Este guia irá ajudá-lo a desenvolver uma classe semelhante à fornecida, do domínio `es.gov.br`, que foi desenvolvida para extrair e fornecer dados sobre a remuneração de servidores públicos e servirá os dados para o monetometro.
Embora o objetivo desse projeto seja a disponibilização de dados públicos para fins de concientização do uso do tempo, outros domínios privados podem ser desenvolvidos seguindo a mesma lógica e serem disponibilizados em ambiente restrito.

## Pré-requisitos

Você deve ter um entendimento básico de Python e estar familiarizado com os pacotes listados no arquivo requirements.txt



## Passos

1. **Defina sua classe**: Comece definindo sua classe e o método `get_remuneracao` que é obrigatório e deve estar presente em sua classe para integração no monetometro. No exemplo fornecido, a classe `Api` é definida e herda de `commons.AbstractETL`.

```python
class Api(AbstractETL):
    def __init__(self):
        super().__init__("es.gov.br")
        self.database = {'guid': None, 'servidores': []}
		
2. **Implemente o método de disponibilização dos dados**:  Implemente método o método obrigatório para disponibilização dos dados da fonte. No exemplo fornecido, o método `get_remuneracao`, obrigatório, é usado para disponibilizar dados de remuneração com base em um endereço de e-mail.

```python
def get_remuneracao(self, email):
    servidor= self.get_remuneracao_por_tipo_origem(email, True)            
    if servidor == None:
        self.print_api("Nenhum servidor encontrado com base no email.")


3. **Implemente métodos de extração e processamento de dados**: Implemente métodos para extrair e processar os dados. No exemplo fornecido, do domínio `es.gov.br`, os métodos `filtrar_e_agrupar_via_api_servidores_por_email e `ler_csv_e_transformar_em_servidores` são usados para processar os dados extraídos. A grande maioria dos dados de transparência são disponibilizados em formato CSV, embora não seja o meio padrão utilizado para extração dos dados no domínio de exemplo, foi desenvolvido o método `ler_csv_e_transformar_em_servidores` que usa o CSV e pode servir de exemplo para outras iniciativas.

4. **Teste sua classe**: Depois de implementar sua classe, teste-a para garantir que ela esteja funcionando corretamente. Você pode fazer isso criando uma instância da classe e chamando seus métodos.

5. **Documente sua classe**: Documente sua classe e seus métodos para que outros desenvolvedores possam entender como usá-la. Você pode fazer isso usando docstrings em Python.
