import json
import requests
import pandas as pd
import unicodedata
import numpy as np
from datetime import datetime
from utils.utils import get_municipio, get_ultimo_ano, add_values


table_name = 'table_numero_de_estabelecimentos_por_tipo_de_exploracao'

cod_regiao_IBGE = [1,2,3,4]

def remover_acentos(texto):
    texto = texto.replace(" - ","_")
    texto = texto.replace(" ","_").lower()
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn'
    )

def df_pivot(df,ano):
    df['ano'] = ano    
    df = df.pivot(index=['codmun', 'ano'], columns='categoria', values='valor')
    df = df.reset_index()  
    df['condicao_do_produtor_em_relacao_as_terras'] = 'Total'  
    df['grupos_de_atividade_economica'] = 'Total'
    return df

def dataframe(ano):
    df = pd.DataFrame(columns=['codmun','categoria','valor'])
    for regiao in cod_regiao_IBGE:
        url = f'https://servicodados.ibge.gov.br/api/v3/agregados/8573/periodos/{ano}/variaveis/40?localidades=N6[N2[{regiao}]]&classificacao=12547[111995,111996,111997,111998,116238,111999,112000,112001,112002,116239,112003,112004]'
        response = requests.get(url)
        retorno = response.text
        retorno = json.loads(retorno)  
        if retorno:
            for dado in retorno[0]['resultados']:
                coluna = dado['classificacoes'][0]['categoria']
                coluna = list(coluna.values())[0]
                categoria = remover_acentos(coluna)
                for valores in dado['series']:
                    codmun = valores['localidade']['id']
                    valor = list(valores['serie'].values())[0]
                    linha_dataframe  = pd.DataFrame([{'codmun':codmun,'categoria':categoria,'valor':valor}])
            
                    df =  pd.concat([df,linha_dataframe], ignore_index=True)
                    df['valor'] = df['valor'].replace('-', 0).astype(int)
    return df
      

def run_table_numero_de_estabelecimentos_por_tipo_de_exploracao():
    try:
        ultimo_ano = get_ultimo_ano(table_name)
        ano_atual = datetime.now().year
        for ano in range(ultimo_ano+1, ano_atual+1):
            df = dataframe(ano)
            mun = get_municipio()
            if df.shape[0]:
                df = df_pivot(df, ano)
                df = pd.merge(df,mun, how='left', on='codmun')
                add_values(df, table_name)

    except Exception as e:
        print(f"Erro na tabela table_percentual_paredes_nao_alvenaria \n{e}")  


