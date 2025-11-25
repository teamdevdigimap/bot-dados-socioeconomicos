import basedosdados as bd
from datetime import datetime
from utils.utils import add_values, get_ultimo_ano, get_municipio
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_despesas_orcamentarias'
ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1
# print("table_despesas_orcamentarias")

#ultimo_ano = 2022

#ano = 2020
def dataframe():
    for ano in range(ultimo_ano, ano_atual):
        try:
            query = f"""
                SELECT
                dados.id_municipio AS id_municipio,
                estagio, 
                conta as Conta,
                valor as Valor, 
                ano
            FROM `basedosdados.br_me_siconfi.municipio_despesas_orcamentarias` AS dados
            LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
                ON dados.id_municipio = diretorio_id_municipio.id_municipio
                WHERE ano = {ano}

            """

            df = bd.read_sql(query, billing_project_id=os.environ['USER'])
            df.columns = ['codmun', 'estagio', 'conta', 'valor', 'ano']
            df_municipios = get_municipio()
            df = df.merge(df_municipios, on='codmun', how='left')
            if df.shape[0]:
                #print(df)
                add_values(df, table_name)
        except Exception as e:
            print(f"Erro para atualizar tabela {table_name}\nErro {e}") 

def run_table_despesas_orcamentarias():
    try:
        dataframe()    
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")  
