import importlib
import os
import importlib.util
from datetime import datetime
import base64

def get_configuration_value(chave, conf_file_path=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),"app.conf")):
    try:
        with open(conf_file_path, 'r') as arquivo:
            for linha in arquivo:
                partes = linha.strip().split(':', 1)
                if len(partes) == 2 and partes[0].strip() == chave:
                    return partes[1].strip()
        print(f'Chave "{chave}" não encontrada no arquivo.')
        return None
    except FileNotFoundError:
        print(f'Arquivo "{conf_file_path}" não encontrado.')
        return None
    except Exception as e:
        print(f'Ocorreu um erro: {e}')
        return None
        

def identificar_dominio(email, pasta_raiz):
    partes_email = email.split('@')

    # Verifica se há pelo menos duas partes após a separação pelo '@'
    if len(partes_email) >= 2:
        dominio = partes_email[-1].lower()
        dominio_com_underscore = dominio.replace('.', '_')
        caminho_arquivo = os.path.join(pasta_raiz, dominio_com_underscore + ".py")

        # Verifica se o arquivo correspondente ao domínio existe
        if os.path.isfile(caminho_arquivo):
            return dominio

        # Se não encontrar, tenta remover subdomínios
        subdominios = dominio.split('.')        
        while len(subdominios) > 1:
            subdominios.pop(0)  # Remove o primeiro subdomínio
            dominio_atualizado = ".".join(subdominios).replace('.', '_')
            caminho_arquivo_atualizado = os.path.join(pasta_raiz, dominio_atualizado + ".py")

            # Verifica se o arquivo correspondente ao domínio atualizado existe
            if os.path.isfile(caminho_arquivo_atualizado):
                return dominio_atualizado.replace('_', '.')

    return None  # Retorna None se o domínio não corresponder a nenhum arquivo na hierarquia

def carregar_script_do_dominio(dominio, pasta_raiz):
    try:
        dominio_com_underscore = dominio.replace('.', '_')
        caminho_script = os.path.join(pasta_raiz, f"{dominio_com_underscore}.py")

        # Verificar se o arquivo existe antes de tentar carregar
        if os.path.exists(caminho_script):
            # Carregar o módulo dinamicamente
            spec = importlib.util.spec_from_file_location(dominio_com_underscore, caminho_script)
            modulo = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(modulo)

            return modulo

    except ImportError as e:
        print(f"Erro ao importar o módulo para o domínio {dominio}: {e}")
        return None


def decode_base64(encoded_string):
    try:
        # Decodificando a string em Base64
        decoded_bytes = base64.b64decode(encoded_string)
        # Convertendo os bytes decodificados de volta para uma string
        decoded_string = decoded_bytes.decode('utf-8')
        return decoded_string
    except Exception as e:
        print(f'Erro ao decodificar Base64: {e}')
        return None
        
def log(texto):
    # Obter a data atual
    data_atual = datetime.now()
    
    # Formatando a data para incluir no nome do arquivo
    nome_arquivo_log = f"log_{data_atual.strftime('%Y-%m-%d')}.txt"

    # Criando o caminho completo para o arquivo de log
    caminho_completo = os.path.join(get_configuration_value("LOG_DIRECTORY"), nome_arquivo_log)

    texto_log = (f"[{datetime.now()}] {texto}\n")

    # Abrir o arquivo no modo "anexar"
    with open(caminho_completo, "a") as arquivo:
        arquivo.write(texto_log)
    
    print(texto_log)