# Guia para Desenvolvimento de uma Classe de Extração de Dados de Remuneração de Servidores Públicos

Este guia irá ajudá-lo a desenvolver uma classe semelhante à fornecida pelo domínio `es.gov.br`, que foi projetada para extrair e fornecer dados sobre a remuneração de servidores públicos, para serem utilizados pelo Monetometro. Embora o objetivo deste projeto seja disponibilizar dados públicos para conscientização sobre o uso do dinheiro público, outras classes para domínios privados podem ser desenvolvidas seguindo a mesma lógica e serem disponibilizadas em ambientes restritos.

## Pré-requisitos

Você deve ter um entendimento básico de Python e estar familiarizado com as seguintes bibliotecas:

- `requests`
- `BeautifulSoup`
- `re`
- `os`
- `urllib`
- `io`
- `pandas`
- `json`

## Passos

1. **Defina sua classe**: Comece definindo sua classe e a função `get_remuneracao`, que é obrigatória e deve estar presente em sua classe para integração com o Monetometro. No exemplo fornecido, a classe `Api` é definida, que herda de `AbstractETL`.

    ```python
    class Api(AbstractETL):
        def __init__(self):
            super().__init__(dominio="es.gov.br",
                             portal_remuneracoes_url=URL_PORTAL_TRANSPARENCIA+PATH_PORTAL_REMUNERACOES,
                             fn_obter_link_mais_recente=self.obter_links_csv_mais_recentes,
                             fn_ler_fonte_de_dados_e_transformar_em_dataframe=self.ler_csv_e_transformar_em_servidores
                             )

        def get_remuneracao(self, email):
            return self.run(email)
    ```

2. **Implemente a função de identificação do link mais recente da fonte dos dados**: No exemplo fornecido, o método `obter_links_csv_mais_recentes` é usado para identificar a fonte dos dados. Cada fonte terá o seu próprio identificador, que pode ser o link, que periodicamente é atualizado, ou uma parte do link que será validada para saber se os dados em cache precisam ser atualizados. A lógica dessa função deve sempre retornar o identificador da fonte de dados mais recente disponível.

    ```python
    def obter_links_csv_mais_recentes(self, num_links=1):
        url_portal = URL_PORTAL_TRANSPARENCIA + PATH_PORTAL_REMUNERACOES
        response = requests.get(url_portal, verify=False)
        .
        .
        .
    ```

3. **Implemente a função de extração dos dados**: Implemente funções para extrair e processar os dados, retornando um DataFrame com a estrutura ['NOME', 'REMUNERACAO_MENSAL_MEDIA', 'ORGAO', 'SIGLA', 'DOMINIO']. No exemplo fornecido, do domínio `es.gov.br`, o método `ler_csv_e_transformar_em_servidores` é usado para ler e processar os dados. A maioria dos dados de transparência é disponibilizada em formato CSV. Se forem outros tipos de arquivo como .odf ou .xls(x), poderão ser usadas outras bibliotecas para extração dos dados. A saída dessa função deve atender à estrutura supracitada.

4. **Teste sua classe**: Após implementar sua classe, teste-a para garantir que esteja funcionando corretamente. Você pode fazer isso criando uma instância da classe e chamando suas funções.

5. **Documente sua classe**: Documente sua classe e suas funções para que outros desenvolvedores possam entender como usá-la. Você pode fazer isso usando docstrings em Python.
