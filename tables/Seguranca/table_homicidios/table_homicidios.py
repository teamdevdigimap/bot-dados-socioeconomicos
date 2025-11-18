import basedosdados as bd
from datetime import datetime
import pandas as pd
from utils.utils import add_values,get_ultimo_ano, get_municipio

table_name = 'table_homicidios'
ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1
#ultimo_ano = 2019

def dataframe():

    for ano in range(ultimo_ano, ano_atual):
        # print(f"Atualizando ano {ano}")
        query = f"""
        SELECT
            dados.id_municipio AS id_municipio,
            quantidade_homicidio_doloso,
            ano,
        FROM `basedosdados.br_fbsp_absp.municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
            WHERE ano = {ano}
        """

        df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')
        df = df.rename(columns={'id_municipio':'codmun'})
        df.columns = ['codmun', 'quantidadehomicidiodoloso', 'ano']
        mun = get_municipio()
        df = pd.merge(df,mun, how='left', on='codmun')
        #print("columns>>>>>>>>>>>>", df.columns)
        if df.shape[0]:
            #print(df)
            add_values(df, table_name)
                

def run_table_homicidios():
    try:
        dataframe()   
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")          