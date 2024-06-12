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
                # Usamos um conjunto (set) para evitar duplicatas
                participantes_set = set()
                for item in component.attendees:
                    participantes_set.add(item.email)
                participantes_set.add(component.organizer.email)
                self.participantes = list(participantes_set)
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
        html = f'<h2 style="margin-top: 0; margin-bottom: 20px;color:#fff">{self.resumo}</h2>'
        html += f'<p style="color:#fff"><strong>Organizador:</strong> {self.organizador}</p>'
        html += f'<p style="color:#fff"><strong>Participantes:</strong> {', '.join(self.participantes)}</p>'
        html += f'<p style="color:#fff"><strong>Iniciando em:</strong> {self.format_data(self.data_inicio)}</p>'
        html += f'<p style="color:#fff"><strong>Terminando em:</strong> {self.format_data(self.data_fim)}</p>'
        html += f'<p style="color:#fff"><strong>Local:</strong> {self.localizacao or ''}</p>'
        html += f'<p style="color:#fff"><strong>Detalhamento:</strong> {self.descricao or ''}</p>'
        #html += f'<p></p><p style="font-size: 16px; line-height: 1.5;"><strong>Acompanhe o custo do evento em tempo real.</strong> </p>'
        
        html_template='<meta charset="UTF-8"><style>body{font-family:Arial,sans-serif;font-size:2.4vw;line-height:1.6}a{color:#f7d579;text-decoration:none}</style><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;background:#000;color:#fff" width="100%" height="100%"><tr><td style="padding:0" valign="top"><div align="center"><table border="0" cellpadding="0" cellspacing="0" role="presentation" width="640"><tr><td><div align="center"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%" width="100%"><tr><td><tr><td><tr><td width="100%"><div align="center"><table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%"><tr><td style="padding:0"><p style="text-align:center" align="center"><img alt="monetometro" src="https://monetometro.com/mh.png" style="border-bottom-style:none;border-bottom-width:0;border-left-style:none;border-left-width:0;border-right-style:none;border-right-width:0;border-top-style:none;border-top-width:0;display:block;max-height:227px" border="0"></table></div><tr><td style="background:#000;color:#fff;padding:0"><div align="center"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:93%;color:#fff" width="93%"><tr style="height:16.5pt"><td style="padding:0;height:16.5pt"><p><span><o:p></o:p></span><tr><td style="padding:0;color:#fff;font-size:15pt">{html_content}<tr><td style="padding:0;height:16.5pt"><p><span><o:p></o:p></span><tr><td style="padding:0;height:16.5pt"><p><span><o:p></o:p></span><tr><td style="background:#000;color:#fff;padding:0"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="color:#fff" align="left"><tr><td style="background:#fab802;color:#fff;padding:7.5pt"><p style="text-align:center" align="center"><a href="{link_monetometro}" target="_blank"><strong><span style="color:#000;text-decoration:none;text-underline:none;font-size:15pt">Abrir meu evento no MONETOMETRO</span></strong></a></table><tr><td style="padding:0;height:16.5pt"><p><span><o:p></o:p></span><tr><td style="padding:0;font-size:15pt">Você também pode copiar esse endereço e usar diretamente no seu navegador:<a href="{link_monetometro}" style="text-decoration:underline;color:#f7d579" target="_blank">{link_monetometro}</a><br><br><tr><td style="padding:0"><p><span><o:p></o:p></span></table></div><tr><td style="background:#000;color:#fff;padding:0;border-top:3px solid #fab802"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;color:#fff" width="100%"><tr><td style="width:18pt;padding:0" width="24"><p><span><o:p></o:p></span><td style="padding:0;color:#fff" valign="top"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="max-width:412.5pt;color:#fff"><tr><td style="padding:0"><div style="padding:0 25px 25px 25px;margin-top:30px"><h2 style="font-size:16pt;margin-bottom:10px">Seja um apoiador</h2><p style="font-size:13pt;margin-bottom:10px">Como qualquer projeto online, o <a href="https://monetometro.com" style="text-decoration:underline;color:#f7d579" target="_blank">MONETOMETRO</a> precisa do seu apoio para continuar crescendo e se desenvolvendo. Ajude-nos a manter o serviço acessível e gratuito para todos os usuários. Contribua para cobrir os custos essenciais e garantir a sustentabilidade do projeto.<p style="font-size:13pt;margin-bottom:10px">Você pode depositar bitcoins diretamente em nossa carteira ou usar uma das outras formas de pagamento disponíveis na plataforma de financiamento coletivo <a href="https://www.kickante.com.br/financiamento-coletivo/monetometro" style="text-decoration:underline;color:#f7d579" target="_blank">Kickante</a>.<p style="font-size:13pt;margin-bottom:10px"><strong>Escolha a melhor opções de pagamento abaixo:</strong><div style="margin-top:10px"><table><tr><td><a href="https://www.kickante.com.br/financiamento-coletivo/monetometro/pagamento" style="margin:0 10px;text-decoration:underline;text-align:center;color:#fff"><img alt="Cartão" src="https://monetometro.com/card.png" style="width:40px"><div style="font-size:10pt">CARTÃO</div></a><td><a href="https://www.kickante.com.br/financiamento-coletivo/monetometro/pagamento" style="margin:0 10px;text-decoration:underline;text-align:center;color:#fff"><img alt="Boleto" src="https://monetometro.com/bar.png" style="width:40px"><div style="font-size:10pt">BOLETO</div></a><td><a href="https://www.kickante.com.br/financiamento-coletivo/monetometro/pagamento?action=securePix" style="margin:0 10px;text-decoration:underline;text-align:center;color:#fff"><img alt="PIX" src="https://monetometro.com/qr.png" style="width:40px"><div style="font-size:10pt">PIX</div></a><td><a href="bitcoin:14m3UeajUTrqgsb3nHV9PaV6U7EocfbLRo?amount=" style="margin:0 10px;text-decoration:underline;text-align:center;color:#fff"><img alt="Bitcoin" src="https://monetometro.com/bitcoin.png" style="width:40px"><div style="font-size:10pt">BITCOIN</div></a><tr><td><tr><td></table></div></div></table></table></table></div></table></div></table>'.replace("{html_content}",html).replace('{link_monetometro}',self.monetometro_url)

        return html_template

    def format_data(self, data):
        if data:
            return datetime.strftime(data, "%d-%m-%Y %H:%M:%S")
        return ""   