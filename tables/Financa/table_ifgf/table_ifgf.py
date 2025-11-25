from datetime import datetime
import basedosdados as bd
import requests
import json
import pandas as pd
from utils.utils import add_values, get_municipio, get_ultimo_ano
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_ifgf'
# Obter o último ano registrado
ultimo_ano = get_ultimo_ano(table_name)
data_atual = datetime.now().year

# print(f"Último ano na tabela: {ultimo_ano}")
# print(f"Ano atual: {data_atual}")


df_municipios = get_municipio()

def dataframe():
    # Verificar necessidade de atualização
    for ano in (ultimo_ano + 1, data_atual+1):
        try:
            #ultimo_ano 
            #print(">>>>>>>>>>>>>>>>>>> ANO", ultimo_ano)
            
            # Consulta os dados da API do Base dos Dados
            query = f"""
            SELECT
                id_municipio AS codmun,  
                indice_firjan_gestao_fiscal AS ifgf,
                sigla_uf,
                ranking_estadual,
                ranking_nacional,
                ano
            FROM 
                `basedosdados.br_firjan_ifgf.ranking` 
            WHERE 
                ano = {ano};
            """

            df = bd.read_sql(query, billing_project_id=os.environ['USER'])

            # Adicionando os nomes dos municípios com merge
            df = df.merge(df_municipios, on='codmun', how='left')

            # Adicionando a data de criação dos registros

            # Inserindo os dados no banco de dados
            df = df.rename(columns={"codmun": "cod_mun7"})
            df = df[["ifgf","ano", "nome_sigla", "cod_mun7"]]
            if df.shape[0]:
                print(df)
                #add_values(df)

        except Exception as error:
            print(f"Erro durante a atualização da tabela: {error}")

    else:
        print(f"A tabela {table_name} já está atualizada.")


def run_table_ifgf():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")