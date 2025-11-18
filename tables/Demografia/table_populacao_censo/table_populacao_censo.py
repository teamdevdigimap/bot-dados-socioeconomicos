from datetime import datetime
import json
import pandas as pd
import requests
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = "table_populacao_censo"

def dataframe():
    ultimo_ano = get_ultimo_ano(table_name) + 1
    ano_atual = datetime.now().year + 1
    #print(">>>>>>>>>>>>>>>>>>",ultimo_ano)
    for ano in range(ultimo_ano, ano_atual):
        url = f'https://servicodados.ibge.gov.br/api/v3/agregados/4709/periodos/{ano}/variaveis/93?localidades=N6[all]'
        response = requests.get(url)
        retorno = response.text
        retorno = json.loads(retorno)
        dados = []
        if retorno:
            for municipio_dados in retorno[0]['resultados'][0]['series']:
                codmun = municipio_dados['localidade']['id']
                pop    =  municipio_dados['serie'][f'{ano}']
                dados.append([codmun, pop])
                
            colunas = ['codmun', 'pop']
            df = pd.DataFrame(dados, columns=colunas) 
            df['ano'] = ano
            df_municipios = get_municipio()
            df = df.merge(df_municipios, on='codmun', how='left')
            #print(df)
            add_values(df, table_name)

def run_table_populacao_censo():
    try:
        dataframe()    
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")  