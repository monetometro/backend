import requests
from time import sleep
import warnings
from commons.utils import get_traceback_string


class HTTPRequestManager:
    def __init__(self, verify_ssl=True):    
        """
        Inicializa o gerenciador de requisições HTTP.

        Parâmetros:
            verify_ssl (bool): Determina se a validação de SSL deve ser realizada. O padrão é True.
        """
        self.verify_ssl = verify_ssl
        self.default_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"}

    def get(self, url, max_attempts=1, expected_status_code=200, headers=None): 
        """
        Realiza uma solicitação HTTP GET.

        Parâmetros:
            url (str): A URL para a qual a solicitação deve ser enviada.
            max_attempts (int): O número máximo de tentativas em caso de falha. O padrão é 1.
            expected_status_code (int): O código de status HTTP esperado como resposta. O padrão é 200.
            headers (dict): Um dicionário de cabeçalhos personalizados a serem enviados com a solicitação. O padrão é None.

        Retorna:
            response (Response): O objeto de resposta da solicitação HTTP, ou None em caso de falha.
        """
        headers = headers or self.default_headers
        attempt = 0
        while attempt < max_attempts:
            try:
                response = requests.get(url, verify=self.verify_ssl, headers=headers)
                if response.status_code == expected_status_code:
                    return response
                else:
                    warnings.warn(f"GET {url} Status de resposta inesperado ({response.status_code}). Tentando novamente...")
            except Exception as e:
                 print(f"GET {url} Erro ao fazer requisição: {e}" + get_traceback_string())
            attempt += 1
            if attempt >= max_attempts:
                return response
            sleep(1)  # Aguardar 1 segundo antes da próxima tentativa
        return None

    def post(self, url, data=None, max_attempts=1, expected_status_code=200, headers=None):
        """
        Realiza uma solicitação HTTP POST.

        Parâmetros:
            url (str): A URL para a qual a solicitação deve ser enviada.
            data (dict): Os dados a serem enviados na solicitação. O padrão é None.
            max_attempts (int): O número máximo de tentativas em caso de falha. O padrão é 1.
            expected_status_code (int): O código de status HTTP esperado como resposta. O padrão é 200.
            headers (dict): Um dicionário de cabeçalhos personalizados a serem enviados com a solicitação. O padrão é None.

        Retorna:
            response (Response): O objeto de resposta da solicitação HTTP, ou None em caso de falha.
        """
        headers = headers or self.default_headers
        attempt = 0
        while attempt < max_attempts:
            try:
                response = requests.post(url, data=data, verify=self.verify_ssl, headers=headers)
                if response.status_code == expected_status_code:
                    return response
                else:
                    warnings.warn(f"POST {url} ({response.status_code}). Tentando novamente...")
            except Exception as e:
                print(f"POST {url} Erro ao fazer requisição: {e}" + get_traceback_string())
            attempt += 1
            if attempt >= max_attempts:
                return response
            sleep(1)  # Aguardar 1 segundo antes da próxima tentativa
        return None


    def head(self, url, max_attempts=1, expected_status_code=200, allow_redirects=True, headers=None):
        """
        Realiza uma solicitação HTTP HEAD.

        Parâmetros:
            url (str): A URL para a qual a solicitação deve ser enviada.
            max_attempts (int): O número máximo de tentativas em caso de falha. O padrão é 1.
            expected_status_code (int): O código de status HTTP esperado como resposta. O padrão é 200.
            allow_redirects (bool): Determina se as redireções devem ser seguidas automaticamente. O padrão é True.
            headers (dict): Um dicionário de cabeçalhos personalizados a serem enviados com a solicitação. O padrão é None.

        Retorna:
            response (Response): O objeto de resposta da solicitação HTTP, ou None em caso de falha.
        """
        headers = headers or self.default_headers
        attempt = 0
        while attempt < max_attempts:
            try:
                response = requests.head(url, verify=self.verify_ssl, allow_redirects=allow_redirects, headers=headers)
                if response.status_code == expected_status_code:
                    return response
                else:
                    warnings.warn(f"HEAD {url} Status de resposta inesperado ({response.status_code}). Tentando novamente...")
            except Exception as e:
                print(f"HEAD {url} Erro ao fazer requisição: {e}" + get_traceback_string())
            attempt += 1
            if attempt >= max_attempts:
                return response 
            sleep(1)  # Aguardar 1 segundo antes da próxima tentativa
        return None
