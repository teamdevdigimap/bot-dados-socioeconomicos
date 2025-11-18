from pysus.online_data.CNES import download
from dateutil.relativedelta import relativedelta
from datetime import datetime
import pandas as pd
from utils.utils import get_municipio_codmun6, get_ultimo_mes_ano, add_values

table_name = 'table_saude_leitos_internacao'

estados_brasil =[
    'AC',  # Acre
    'AL',  # Alagoas
    'AM',  # Amazonas
    'AP',  # Amapá
    'BA',  # Bahia
    'CE',  # Ceará
    'DF',  # Distrito Federal
    'ES',  # Espírito Santo
    'GO',  # Goiás
    'MA',  # Maranhão
    'MG',  # Minas Gerais
    'MS',  # Mato Grosso do Sul
    'MT',  # Mato Grosso
    'PA',  # Pará
    'PB',  # Paraíba
    'PE',  # Pernambuco
    'PI',  # Piauí
    'PR',  # Paraná
    'RJ',  # Rio de Janeiro
    'RN',  # Rio Grande do Norte
    'RO',  # Rondônia
    'RR',  # Roraima
    'RS',  # Rio Grande do Sul
    'SC',  # Santa Catarina
    'SE',  # Sergipe
    'SP',  # São Paulo
    'TO'   # Tocantins
]

def run_table_saude_leitos_internacao():
    try:
        mun = get_municipio_codmun6()
        mun.columns = ['codmun7','codmun','nome_sigla']
        mes, ano = get_ultimo_mes_ano(table_name)
        ultima_data = datetime(ano, mes, 1)
        ultima_data = ultima_data + relativedelta(months=1)
        data_atual = datetime.now() - relativedelta(months=1)
        while ultima_data < data_atual:
            ano = ultima_data.year
            mes = ultima_data.month
            for estado in estados_brasil:
                df = download(group='LT', year=ano, month=mes, state=estado)
                df = df.groupby('CODUFMUN')['QT_EXIST'].sum().reset_index()
                df['mes'] = mes
                df['ano'] = ano
                df = df.rename(columns={'CODUFMUN':'codmun','QT_EXIST':'numerodeleitosinternacoes'})
                df = pd.merge(df,mun,how='left', on='codmun')  
                if df.shape[0]:
                    add_values(df, table_name)  
            
            ultima_data = ultima_data + relativedelta(months=1)
        
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")      