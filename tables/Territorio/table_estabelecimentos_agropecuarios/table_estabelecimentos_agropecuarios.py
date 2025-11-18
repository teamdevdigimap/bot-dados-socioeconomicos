import pandas as pd
import requests
import json
from utils.utils import get_municipio, get_ultimo_ano, add_values
from datetime import datetime

table_name = "table_estabelecimentos_agropecuarios"


def dataframe(ano):
    url = f'https://servicodados.ibge.gov.br/api/v3/agregados/9053/periodos/{ano}/variaveis/183?localidades=N6[all]'
    response = requests.get(url)
    retorno = response.text
    retorno = json.loads(retorno)  
    df = pd.DataFrame(columns=['codmun', 'numero_estabelecimentos_agropecuarios'])
    if retorno:
        for dado in retorno[0]['resultados'][0]['series']:
            codmun = dado['localidade']['id']
            numero_estabelecimentos_agropecuarios = list(dado['serie'].values())[0]
            linha_dataframe  = pd.DataFrame([{'codmun':codmun,'numero_estabelecimentos_agropecuarios':numero_estabelecimentos_agropecuarios}])
            df =  pd.concat([df,linha_dataframe], ignore_index=True)
        df['numero_estabelecimentos_agropecuarios'] = df['numero_estabelecimentos_agropecuarios'].replace('-', 0).astype(int)
        df['ano'] = ano

    return df

def run_table_estabelecimentos_agropecuarios():
      try:
          ultimo_ano = get_ultimo_ano(table_name)
          ano_atual = datetime.now().year
          mun = get_municipio()
          for ano in range(ultimo_ano+1, ano_atual+1):
            df = dataframe(ano)
            if df.shape[0]:
                df = pd.merge(df, mun, how='left')
                add_values(df, table_name)
      except Exception as e:
        print(f"{table_name} \n{e}")  
