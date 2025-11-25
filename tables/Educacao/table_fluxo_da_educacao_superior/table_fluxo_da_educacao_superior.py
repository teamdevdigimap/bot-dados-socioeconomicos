import basedosdados as bd
from datetime import datetime
import pandas as pd
import psycopg2
from utils.utils import add_values, get_ultimo_ano, get_municipio
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_fluxo_da_educacao_superior'

ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1

def dataframe():
    for ano in range(ultimo_ano, ano_atual):
        query = f"""
        SELECT
        dados.id_municipio AS codmun,
        dados.ano_referencia AS ano,
    SUM(dados.quantidade_permanencia) AS `Permanência`,
        SUM(dados.quantidade_concluinte) AS `Concluintes`,
        SUM(dados.quantidade_desistencia) AS `Desistências`
        FROM
            basedosdados.br_inep_indicadores_educacionais.fluxo_educacao_superior AS dados
        WHERE
            dados.ano_referencia = {ano}
        GROUP BY
            dados.id_municipio, dados.ano_referencia;
        """
        df = bd.read_sql(query, billing_project_id=os.environ['USER'])

        if df.shape[0]:
            df = df.melt(id_vars=['codmun', 'ano'], value_vars=['Permanência', 'Concluintes', 'Desistências'], 
                    var_name='categoria', value_name='quantidade')
            mun = get_municipio()

            df = pd.merge(df, mun, on='codmun', how='left')
            #print(df)
            add_values(df, table_name)

def run_table_fluxo_da_educacao_superior():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}") 