import ssl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
import pandas as pd
from utils.utils import add_values, get_municipio, get_ultimo_mes_ano, codmun_siglauf, get_codmun

ssl._create_default_https_context = ssl._create_unverified_context

table_name = "table_fundo_de_participacao_dos_municipios"


def get_df_soma(url):
    df = pd.read_csv(url, encoding='ISO-8859-1', sep=';')
    df = df[df['Transferência'] == 'FPM']
    df['valor'] = df['1º Decêndio'] + df['2º Decêndio'] + df['3º Decêndio']
    df = df.groupby(['Município', 'UF', 'ANO', 'Mês'])['valor'].sum().reset_index()
    return df

def dataframe():
    mun = get_municipio()
    mes,ano = get_ultimo_mes_ano(table_name)
    ultima_data = datetime(ano, mes, 1)
    ultima_data = ultima_data + relativedelta(months=1)
    data_atual = datetime.now() - relativedelta(months=1)
    codmun = get_codmun()
    url = "http://www.tesourotransparente.gov.br/ckan/api/3/action/package_show?id=transferencias-constitucionais-para-municipios"
    resposta = requests.get(url).text
    resposta = json.loads(resposta)

    while ultima_data < data_atual:
        MES = ultima_data.month
        ANO = ultima_data.year
        for i in resposta['result']['resources']:
            if i['name'] and ('Transferencia_Mensal_Municipios' in i['name']):
                name = i['name']
                ano = name.split("_")[-1].split(".csv")[0][:4]
                mes = name.split("_")[-1].split(".csv")[0][4:]
                if int(ano) == ANO:
                    df = get_df_soma(i['url'])
                    df = df[df['Mês'] == MES]
                    if df.shape[0]:
                        df = pd.merge(codmun, df, left_on=['nomemuni', 'siglaestado'], right_on=['Município', 'UF'], how='right')
                        df = df[['codmun', 'valor', 'Mês', 'ANO']]
                        df.columns = ['codmun', 'valor', 'mes', 'ano']
                        df = df[df['codmun'].notna()] # Remove as linhas com codmun null
                        df = pd.merge(df,mun,how='left', on='codmun')
                        add_values(df,table_name)
        ultima_data = ultima_data + relativedelta(months=1)

def run_table_fundo_de_participacao_dos_municipios():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")
       