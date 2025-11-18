from datetime import datetime
import json
import pandas as pd
import requests
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = "table_densidade_domiciliar"

def dataframe():
    #print(">>>>>>>>>>>>>>>>tabela", table_name)
    ultimo_ano = get_ultimo_ano(table_name) + 1
    ano_atual = datetime.now().year + 1

    for ano in range(ultimo_ano, ano_atual+1):
    
        #print(f'Ano - {ano}')
        url = f'https://servicodados.ibge.gov.br/api/v3/agregados/4712/periodos/{ano}/variaveis/381|382|5930?localidades=N6[all]'

        response = requests.get(url)
        retorno = response.text
        retorno = json.loads(retorno)
        #print(">>>>>>>>>>>>> retorno", retorno)
        if retorno:
            r = retorno[0]
            r['variavel'] #retorno[1] retorno[2]
            r['resultados'][0]['series']


            columns = ['codmun']

            for variavel in retorno:
                categoria = variavel['variavel']
                columns.append(categoria)
                
            df = pd.DataFrame(columns=columns)

            for variavel in retorno: 
                categoria = variavel['variavel']
                resultados = variavel['resultados'][0]['series']
                
                for resultado in resultados:
                    codmun  = resultado['localidade']['id']
                    valor = resultado['serie'][f'{ano}']
                    row = {
                        'codmun'       : codmun,
                        f'{categoria}' : valor
                    }
                    
                    row = pd.DataFrame([row])
                    df = pd.concat([df, row], ignore_index=True)        
                    # print(f"{categoria} -> {codmun} : {resultado['serie'][f'{ano}']}")    

            df_municipios = get_municipio()
            #print(df_municipios)
            df = df.groupby('codmun').first().reset_index()   

            df['ano'] = ano

            df.columns = ['codmun','domicilios','pop','densidade','ano']
            df = df.merge(df_municipios, on='codmun', how='left')
            #print(df)
            add_values(df, table_name)


def run_table_densidade_domiciliar():
    try:
       # print(">>>>>>>>>>>>>>> dataframe", dataframe)
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")  