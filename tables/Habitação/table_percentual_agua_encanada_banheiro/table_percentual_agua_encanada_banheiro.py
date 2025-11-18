import pandas as pd
import json
import requests
from utils.utils import get_municipio, get_ultimo_ano, add_values


table_name = "table_percentual_agua_encanada_banheiro"
def dataframe():
    ano = get_ultimo_ano(table_name) + 1
    url = f'https://servicodados.ibge.gov.br/api/v3/agregados/9397/periodos/{ano}/variaveis/1000382?localidades=N6[all]&classificacao=458[12032]|11558[46290]|58[95253]|86[95251]'

    response = requests.get(url)
    retorno = response.text
    retorno = json.loads(retorno)

    if len(retorno):
        retorno[0]['resultados'][0]['series']
        listas = []
        for l in retorno[0]['resultados'][0]['series']:
            codmun    = l['localidade']['id']
            municipio = l['localidade']['nome'].split(' - ')[0]
            for ano, valor in l['serie'].items():
                listas.append([codmun, municipio, valor, ano])
        colunas = ['codmun','municipio','valor','ano']
        df = pd.DataFrame(listas, columns=colunas)
        add_values(df, table_name)

def run_table_percentual_agua_encanada_banheiro():
    try:
        dataframe()    
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")    