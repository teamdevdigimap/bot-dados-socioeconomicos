import requests
import pandas as pd
from utils.utils import add_values,get_ultimo_mes_ano, get_municipio_codmun6
from datetime import datetime

table_name = "table_bolsa_familia"


def dataframe():
    mes, ano = get_ultimo_mes_ano(table_name)
    mun = get_municipio_codmun6()
    mun = mun.rename(columns={'codmun':'codmun7','codmun6':'codmun'})
    url = f'https://aplicacoes.mds.gov.br/sagi/servicos/misocial/?fq=anomes_s:{ano}*&fl=codigo_ibge%2Canomes_s%2Cqtd_familias_beneficiarias_bolsa_familia_s%2Cvalor_repassado_bolsa_familia_s%2Cpbf_vlr_medio_benef_f&fq=valor_repassado_bolsa_familia_s%3A*&q=*%3A*&rows=100000&sort=anomes_s%20desc%2C%20codigo_ibge%20asc&wt=csv'
    response = requests.get(url)
    retorno = response.text.split("\n")
    ano_soma = 1
    while len(retorno) > 2:
        colunas = retorno[0].split(',')
        dados = [linha.split(',') for linha in retorno[1:]]
        # Criar o DataFrame usando as colunas e dados
        df = pd.DataFrame(dados, columns=colunas)
        df = df.rename(columns={'codigo_ibge':'codmun'})
        df.columns = df.columns.str.replace('_', '')
        df['anomess'] = pd.to_datetime(df['anomess'], format='%Y%m')
        data_referencia = datetime(year=ano, month=mes, day=1)
        df = df[df['anomess'] > f'{data_referencia}']  
        df['ano'] = df['anomess'].dt.year
        df['mes'] = df['anomess'].dt.month
        df = df.drop(columns=['anomess'])
        df = df.dropna()
        
        df['codmun'] = df['codmun'].astype(str)
        if df.shape[0]:
            #print("Atualizando dados BPC...")
            for col in df.columns[1:]:
                df[col] = df[col].astype(float)
            df['ano'] = df['ano'].astype(int)
            df['mes'] = df['mes'].astype(int)  
            df = pd.merge(df,mun, how='left', on='codmun') 
            add_values(df, table_name)
            

        url = f'https://aplicacoes.mds.gov.br/sagi/servicos/misocial/?fq=anomes_s:{ano + ano_soma}*&fl=codigo_ibge%2Canomes_s%2Cqtd_familias_beneficiarias_bolsa_familia_s%2Cvalor_repassado_bolsa_familia_s%2Cpbf_vlr_medio_benef_f&fq=valor_repassado_bolsa_familia_s%3A*&q=*%3A*&rows=100000&sort=anomes_s%20desc%2C%20codigo_ibge%20asc&wt=csv'
        response = requests.get(url)
        retorno = response.text.split("\n")
        ano_soma = ano_soma + 1    



def run_table_bolsa_familia():
    try:
        dataframe()   
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")          