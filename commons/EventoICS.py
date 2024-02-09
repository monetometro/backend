import re
from datetime import datetime, timezone, timedelta
import pytz
import tzlocal
from ics import Calendar, Event


class EventoICS:
    def __init__(self, conteudo_ics):
        self.organizador = None
        self.participantes = []
        self.data_inicio = None
        self.data_fim = None
        self.localizacao = None
        self.descricao = None
        self.resumo = None
        self.conteudo_ics = conteudo_ics
        self._ID = None
        self.monetometro_url = None

        self.fill_data(conteudo_ics)

    def fill_data(self, conteudo_ics):

        try:
            cal = Calendar(conteudo_ics)
            for component in cal.events:
                self.organizador = component.organizer.email
                for item in component.attendees:
                    self.participantes.append(item.email)
                self.data_inicio = component.begin.datetime
                self.data_fim = component.end.datetime
                self.localizacao = component.location
                self.descricao = component.description
                self.resumo = component.name
                self._ID = component.uid
        except Exception as e:        
            print('ICS inválido!')

    
    def add_monetometro_url(self, url):
        self.monetometro_url = url

    def get_monetometro_url(self):
        return self.monetometro_url

    def to_ics_content(self):
        return self.conteudo_ics

    def to_html(self):
        html = f"<h2>{self.resumo}</h2>"
        html += f"<p><strong>Organizador:</strong> {self.organizador}</p>"
        html += f"<p><strong>Participantes:</strong> {', '.join(self.participantes)}</p>"
        html += f"<p><strong>Data de Início:</strong> {self.format_data(self.data_inicio)}</p>"
        html += f"<p><strong>Data de Fim:</strong> {self.format_data(self.data_fim)}</p>"
        html += f"<p><strong>Local:</strong> {self.localizacao or ''}</p>"
        html += f"<p><strong>Descrição:</strong> {self.descricao or ''}</p>"
        html += f"<p><strong>Acompanhe em tempo real o custo médio do evento nesse link do MONETOMETRO:</strong> {self.monetometro_url}</p>"
        return html

    def format_data(self, data):
        if data:
            return datetime.strftime(data, "%d-%m-%Y %H:%M:%S")
        return ""