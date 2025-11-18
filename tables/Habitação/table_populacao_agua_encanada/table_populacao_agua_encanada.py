import pandas as pd
from utils.utils import add_values, get_ultimo_ano, get_municipio
import requests
import json

table_name = 'table_populacao_agua_encanada'

def dataframe():
    ano = get_ultimo_ano(table_name) + 1
    url = f'https://servicodados.ibge.gov.br/api/v3/agregados/6909/periodos/{ano}/variaveis/1000382?localidades=N6[all]&classificacao=301[72053]|1817[72126,72127]|58[95253]|86[95251]'
    response = requests.get(url)
    retorno = response.text

    retorno = json.loads(retorno)

    mun = get_municipio()
    mun.columns = ['codmun', 'municipio']

    if len(retorno):
        retorno[0]['resultados'][0]['series']
        listas = []
        for l in retorno[0]['resultados'][0]['series']:
            codmun    = l['localidade']['id']
            municipio = l['localidade']['nome'].split(' - ')[0]
            for ano, valor in l['serie'].items():
                listas.append([codmun, municipio, valor, ano])

        # listas    

        colunas = ['codmun','municipio','valor','ano']
        df = pd.DataFrame(listas, columns=colunas)
        
        if df.shape[0]:
            # print(f"Atualizando dados População Agua encanada,  ano: {ano} ...")
                # Cria a URL de conexão
            df = df.drop(columns=['municipio'])
            df = pd.merge(df,mun, how='left', on='codmun')    
            add_values(df, table_name)


def run_table_populacao_agua_encanada():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}") 