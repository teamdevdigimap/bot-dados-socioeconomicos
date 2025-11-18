from datetime import datetime
import basedosdados as bd
from utils.utils import add_values, get_ultimo_ano, get_municipio
import pandas as pd
table_name = 'table_receita_corrente_liquida'

mun = get_municipio()
ultimo_ano = get_ultimo_ano(table_name) + 1

ano_atual = datetime.now().year + 1


def dataframe():
    for ano in range(ultimo_ano, ano_atual):
        
        try:
            query = f"""
            SELECT
                dados.ano as ano,
                dados.id_municipio AS codmun,
                round(sum(dados.valor),2) as valor
            FROM `basedosdados.br_me_siconfi.municipio_receitas_orcamentarias` AS dados    
            WHERE ano = {ano}
            GROUP BY  id_municipio, ano
            """

            df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')
            #df.columns = ['ano', 'codmun', 'conta', 'valor']
            #df.columns = ['ano', 'codmun','valor']
            if df.shape[0]:
                df = pd.merge(df,mun,on='codmun', how='left')
                df = df.dropna()
                add_values(df,table_name)
        except Exception as e:
            print(f"Erro ao cadastrar os dados na tabela {table_name}\nErro {e}")   


def run_table_receita_corrente_liquida():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")  
