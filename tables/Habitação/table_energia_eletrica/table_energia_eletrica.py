import pandas as pd
import requests
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = 'table_energia_eletrica'

def dataframe():
    
    mun = get_municipio()
    mun.columns = ['codmun', 'municipio']
    ano = get_ultimo_ano(table_name) + 1
    url = f'https://servicodados.ibge.gov.br/api/v3/agregados/3513/periodos/{ano}/variaveis/1000137?localidades=N6[all]&classificacao=12236[0]|125[0]|63[0]|137[0]|309[3011]'
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        if len(data):
            resultados = data[0]['resultados']
            
            all_data = []
            
            for resultado in resultados:
                # classificacoes = resultado['classificacoes']
                series = resultado['series']
                
                for item in series:
                    localidade = item['localidade']
                    valores = item['serie']
                    
                    for ano, valor in valores.items():
                        all_data.append({
                            'codmun': localidade['id'],
                            'municipio': localidade['nome'],
                            #'classificacao_id': classificacoes[0]['id'] if classificacoes else None,
                            #'classificacao_nome': classificacoes[0]['nome'] if classificacoes else None,
                            'ano': ano,
                            'valor': valor
                        })
            
            df = pd.DataFrame(all_data, columns=['codmun','municipio','ano','valor'])
            if df.shape[0]:
                add_values(df, table_name)
                #df = df.drop(columns=['municipio'])
                #df = pd.merge(df, mun, how='left', on='codmun')
    
def run_table_energia_eletrica():
    try:
        dataframe()    
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")  