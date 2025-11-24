from datetime import datetime
import requests
import pandas as pd
from utils.utils import add_values,get_ultimo_mes_ano, get_municipio_codmun6

table_name = 'table_bpc'

def dataframe():
    mes,ano = get_ultimo_mes_ano(table_name)
    mun = get_municipio_codmun6()
    mun['codmun'] = mun['codmun'].astype(str)   # Código de 7 dígitos
    mun['codmun6'] = mun['codmun6'].astype(str) # Código de 6 dígitos
    url = f"https://aplicacoes.mds.gov.br/sagi/servicos/misocial?fq=anomes_s:{ano}*&fq=tipo_s:mes_mu&wt=csv&q=*&rows=100000000&sort=anomes_s%20asc,%20codigo_ibge%20asc&fl=ibge:codigo_ibge,anomes:anomes_s,bpc_ben:bpc_ben_i,bpc_pcd_ben:bpc_pcd_ben_i,bpc_idoso_ben:bpc_idoso_ben_i,bpc_pcd_val:bpc_pcd_val_s,bpc_idoso_val:bpc_idoso_val_s,bpc_val:bpc_val_s"
    # print("Iniciando Requisição BPC")
    response = requests.get(url)
    retorno = response.text.split("\n")
    ano_soma = 1

    while len(retorno) > 2:
        # print('entrou no while')
        colunas = retorno[0].split(',')
        dados = [linha.split(',') for linha in retorno[1:]]
        # Criar o DataFrame usando as colunas e dados
        df = pd.DataFrame(dados, columns=colunas)
        df = df.rename(columns={'ibge':'codmun'})
        df.columns = df.columns.str.replace('_', '')
        df['anomes'] = pd.to_datetime(df['anomes'], format='%Y%m')
        data_referencia = datetime(year=ano, month=mes, day=1)
        df = df[df['anomes'] > f'{data_referencia}'] 
        df['ano'] = df['anomes'].dt.year
        df['mes'] = df['anomes'].dt.month
        df = df.drop(columns=['anomes'])
        df = df.dropna()
        df = df[~(df.applymap(lambda x: x == '').any(axis=1))]
        df['ano'] = df['ano'].astype(int)
        df['mes'] = df['mes'].astype(int)
        # df['codmun'] = df['codmun'].astype(str)
        if df.shape[0]:
            print("Atualizando dados BPC...")

            df['ano'] = df['ano'].astype(int)
            df['mes'] = df['mes'].astype(int)
            df['codmun'] = df['codmun'].astype(str) 
            cols_to_float = [c for c in df.columns if c not in ['codmun', 'ano', 'mes', 'codmun7', 'nome_sigla']]

            for col in cols_to_float:
                df[col] = df[col].astype(float)

            df = pd.merge(df, mun[['codmun', 'codmun6', 'nome_sigla']], on='codmun', how='left')
            df['codmun7'] = df['codmun']

            df['codmun'] = df['codmun6']

            df = df.drop(columns=['codmun6'])
            df = df.dropna(subset=['codmun'])

            print("Exemplo de linha processada: ", df.iloc[2].to_dict())
            add_values(df, table_name)
        

        url = f"https://aplicacoes.mds.gov.br/sagi/servicos/misocial?fq=anomes_s:{ano + ano_soma}*&fq=tipo_s:mes_mu&wt=csv&q=*&rows=100000000&sort=anomes_s%20asc,%20codigo_ibge%20asc&fl=ibge:codigo_ibge,anomes:anomes_s,bpc_ben:bpc_ben_i,bpc_pcd_ben:bpc_pcd_ben_i,bpc_idoso_ben:bpc_idoso_ben_i,bpc_pcd_val:bpc_pcd_val_s,bpc_idoso_val:bpc_idoso_val_s,bpc_val:bpc_val_s"
        response = requests.get(url)
        retorno = response.text.split("\n")
        ano_soma = ano_soma + 1    

def run_table_bpc():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")          
