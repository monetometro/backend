import json
import requests
import pyodbc
import sqlalchemy as sa
from sqlalchemy import create_engine

# Configuração do banco de dados
driver = '{SQL Server}'
server = 'localhost'
database = 'MSSQLDB'
username = 'sa'
password = '1234'


import urllib

# Função que se conecta com o banco de dados
def connect_db():
    # Criar a string de conexão
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    # Escapar os caracteres especiais
    conn_str = urllib.parse.quote_plus(conn_str)
    # Criar o objeto do banco de dados
    db = create_engine(f'mssql+pyodbc:///?odbc_connect={conn_str}')
    # Retornar o objeto do banco de dados
    return db

# Função que salva os dados JSON de EventoICS no banco de dados SQL Server
def save_data(data,salaries):
    # Conectar com o banco de dados
    db = connect_db()
    # Extrair as propriedades do JSON de EventoICS
    organizer = data['organizador']
    participants = data['participantes']
    start_date = data['data_inicio']
    end_date = data['data_fim']
    location = data['localizacao']
    summary = data['resumo']
    content_ics = data['conteudo_ics']
    # Inserir os dados do evento na tabela Events
    db.execute(f"INSERT INTO Events (Organizer, StartDate, EndDate, Location, Summary, ContentICS) VALUES ('{organizer}', '{start_date}', '{end_date}', '{location}', '{summary}', '{content_ics}')")
    # Obter o id do evento inserido
    event_id = db.execute("SELECT @@IDENTITY AS id").fetchone()[0]
    # Inserir os dados dos participantes na tabela Participants
    for participant in participants:
        db.execute(f"INSERT INTO Participants (Email, EventId) VALUES ('{participant}', {event_id})")
    for salario in salaries:
        # Extrair o email e o valor do salário de cada dicionário
        email = salario['email']
        valor = salario['valor']
        # Inserir os dados do salário na tabela Salaries
        db.execute(f"INSERT INTO Salaries (ParticipantId, Amount, EventId) SELECT Id, {valor}, {event_id} FROM Participants WHERE Email = '{email}'")

