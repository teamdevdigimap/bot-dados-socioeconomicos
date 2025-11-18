from datetime import datetime
import basedosdados as bd
import requests
import json
import pandas as pd
from utils.utils import add_values, get_municipio, get_ultimo_mes_ano

table_name = "table_capacidade_de_pagamento_capag_municipios"

mun = get_municipio()
ano_atual, mes_atual = get_ultimo_mes_ano(table_name)

def dataframe():
    try:
        colunas = ['codmun', 'indicador1', 'nota1', 'indicador2', 'nota2', 'indicador3', 'nota3', 'classificacaocapag', 'mes', 'ano']
        df_final = pd.DataFrame(columns=colunas)
        links = []
        datas = []
        url = "http://www.tesourotransparente.gov.br/ckan/api/3/action/package_show?id=capag-municipios"
        
        resposta = requests.get(url).json()
        recursos = resposta['result']['resources']
        
        data = datetime.fromisoformat(recursos[0]['created'])
        link_xls = ''
        mes_antigo = 0
        
        for xlsx in recursos:
            nova_data = datetime.fromisoformat(xlsx['created'])
            if nova_data > data:
                data = nova_data
                ano = data.year
                mes = data.month
                if ano >= ano_atual and mes != mes_antigo and mes > mes_atual:
                    mes_antigo = mes
                    link_xls = xlsx['url']
                    links.append(link_xls)
                    datas.append(data)
        
        for url, data in zip(links, datas):
            df = pd.read_excel(url, skiprows=2, usecols='A:K')
            df = df.drop(columns=['Nome_Município', 'UF', 'ICF'])
            df = df[['Código Município Completo', 'Indicador 1', 'Nota 1', 'Indicador 2', 'Nota 2', 'Indicador 3', 'Nota 3', 'CAPAG']]
            df.columns = ['codmun', 'indicador1', 'nota1', 'indicador2', 'nota2', 'indicador3', 'nota3', 'classificacaocapag']
            df['mes'] = data.month
            df['ano'] = data.year
            df_final = pd.concat([df, df_final], ignore_index=True)
        
        df_final['nota1'] = df_final['nota1'].astype(str)
        df_final['nota2'] = df_final['nota2'].astype(str)
        df_final['nota3'] = df_final['nota3'].astype(str)
        df_final['indicador1'] = pd.to_numeric(df_final['indicador1'], errors='coerce').fillna(0)
        df_final['indicador2'] = pd.to_numeric(df_final['indicador2'], errors='coerce').fillna(0)
        df_final['indicador3'] = pd.to_numeric(df_final['indicador3'], errors='coerce').fillna(0)
        
        if df_final.shape[0]:
            df_final['codmun'] = df_final['codmun'].astype(str)
            df_final = pd.merge(df_final, mun, how='left', on='codmun')
            df_final = df_final.dropna(subset=['nome_sigla'])
            #print(df_final)
            add_values(df_final, table_name)
    except Exception as e:
        print(f"Erro ao cadastrar os dados na tabela {table_name}\nErro {e}")

def run_table_capacidade_de_pagamento_capag_municipios():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")