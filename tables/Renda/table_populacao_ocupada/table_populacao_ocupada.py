import pandas as pd
from utils.utils import get_ultimo_ano, add_values, get_municipio
import requests
import json


table_name = "table_populacao_ocupada"


def dataframe():
    mun = get_municipio()
    mun.columns = ['codmun','municipio']
    ano = get_ultimo_ano(table_name) + 1
    url = f'https://servicodados.ibge.gov.br/api/v3/agregados/9509/periodos/{ano}/variaveis/707?localidades=N6'
    response = requests.get(url)
    retorno = response.text
    retorno = json.loads(retorno)

    if len(retorno):
        print(f"Atualizando dados População Ocupada ano: {ano} ...")
        retorno[0]['resultados'][0]['series']
        listas = []
        for l in retorno[0]['resultados'][0]['series']:
            codmun    = l['localidade']['id']
            municipio = l['localidade']['nome'].split(' - ')[0]
            for ano, populacaoocupada in l['serie'].items():
                listas.append([codmun, municipio, populacaoocupada, ano])

        # listas    

        colunas = ['codmun','municipio','populacaoocupada','ano']
        df = pd.DataFrame(listas, columns=colunas)
        df = df.drop(columns=['municipio'])
        df = pd.merge(df,mun, on = 'codmun', how='left')

        if df.shape[0]:
            add_values(df, table_name)



def run_table_populacao_ocupada():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")             