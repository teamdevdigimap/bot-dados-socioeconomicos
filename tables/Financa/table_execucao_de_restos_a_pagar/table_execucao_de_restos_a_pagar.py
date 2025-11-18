from datetime import datetime
from utils.utils import add_values, get_ultimo_ano, get_municipio
import requests
import pandas as pd

table_name = 'table_execucao_de_restos_a_pagar'
ultimo_ano = get_ultimo_ano(table_name)
ano_atual = datetime.now().year

def get_valor_a_pagar(data):
    return sum(d['valor'] for d in data['items'] if d.get('coluna') == 'Restos a Pagar Processados Pagos')

def dataframe():
    dados = []
    codmuns = get_municipio()
    mun = get_municipio()
    #codmuns = codmuns.head()
    #print(">>>>>>>>>>>> codmuns", codmuns)
    for ano in range(ultimo_ano+1, ano_atual+1):
        for indice, codmun in enumerate(codmuns['codmun'], start=1):
            #print(f"Municipio {indice} de {len(codmuns)}")

            url = f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt//dca?an_exercicio={ano}&no_anexo=&id_ente={codmun}"
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                if 'items' in data and data['items']:
                    valor = get_valor_a_pagar(data)
                    dados.append({'codmun': codmun, 'restosapagar': valor, 'ano': ano})
                # else:
                #     print(f"Sem dados para {codmun} no ano {ano}")
            else:
                print(f"Erro ao acessar {url}: {response.status_code}")

    df = pd.DataFrame(dados)
    if not df.empty:
        df = pd.merge(df, mun, how='left')

        #print(df)
        add_values(df,table_name)

def run_table_execucao_de_restos_a_pagar():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")  
