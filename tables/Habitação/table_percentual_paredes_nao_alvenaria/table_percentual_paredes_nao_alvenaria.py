import pandas as pd
import requests
from utils.utils import add_values, get_municipio, get_ultimo_ano
from datetime import datetime
import json

table_name = 'table_percentual_paredes_nao_alvenaria'


def dataframe():
    ultimo_ano = get_ultimo_ano(table_name) + 1
    ano_atual = datetime.now().year
    for ano in range(ultimo_ano,ano_atual+1):
        url = f'https://servicodados.ibge.gov.br/api/v3/agregados/3497/periodos/{ano}/variaveis/1000137?localidades=N6[all]&classificacao=1[6795]|137[12195,2873,12196,2875,2876]|65[95810]|74[95811]|471[13234]'
        response = requests.get(url)
        retorno = response.text
        mun = get_municipio()
        retorno = json.loads(retorno)
        listas = []
        if retorno:
            for m in retorno[0]['resultados']:    
                for l in m['series']:
                    codmun    = l['localidade']['id']
                    municipio = l['localidade']['nome'].split(' - ')[0]
                    for ano, percentual in l['serie'].items():
                            listas.append([codmun, municipio, percentual, ano])

            colunas = ['codmun','municipio','percentual','ano']
            df = pd.DataFrame(listas, columns=colunas)   
            df['percentual'] = df['percentual'].replace('-', '0').astype(float)        

            df = df.groupby(['codmun','municipio','ano'])['percentual'].sum().reset_index()

            if df.shape[0]:
                add_values(df,table_name)
        

def run_table_percentual_paredes_nao_alvenaria():
    try:
        dataframe()
    except Exception as e:
        print(f"Erro na tabela table_percentual_paredes_nao_alvenaria \n{e}")  
