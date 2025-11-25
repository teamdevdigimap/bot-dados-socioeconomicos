import basedosdados as bd
import pandas as pd
import numpy as np
from datetime import datetime
from utils.utils import get_municipio, add_values, get_ultimo_ano
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_saude_obitos_prematuros'


def dataframe(ano):
    query_de_prematuros = f"""
    WITH 
    dicionario_tipo_obito AS (
        SELECT
            chave AS chave_tipo_obito,
            valor AS descricao_tipo_obito
        FROM `basedosdados.br_ms_sim.dicionario`
        WHERE
            TRUE
            AND nome_coluna = 'tipo_obito'
            AND id_tabela = 'microdados'
    )
    SELECT
        dados.ano as ano,
        -- descricao_tipo_obito AS tipo_obito,
        dados.id_municipio_ocorrencia AS codmun,
        count(*) as obitosdeprematuros
    FROM `basedosdados.br_ms_sim.microdados` AS dados
    LEFT JOIN `dicionario_tipo_obito`
        ON dados.tipo_obito = chave_tipo_obito
    WHERE ano = {ano} and (descricao_tipo_obito = 'Fetal' OR DATE_DIFF(dados.data_obito, dados.data_nascimento, MONTH) <= 7)
    GROUP BY
    dados.ano,
    -- descricao_tipo_obito,
    dados.id_municipio_ocorrencia
    """

    query_de_30_69 = f"""
    WITH 
    dicionario_tipo_obito AS (
        SELECT
            chave AS chave_tipo_obito,
            valor AS descricao_tipo_obito
        FROM `basedosdados.br_ms_sim.dicionario`
        WHERE
            TRUE
            AND nome_coluna = 'tipo_obito'
            AND id_tabela = 'microdados'
    )
    SELECT
        dados.ano as ano,
        -- descricao_tipo_obito AS tipo_obito,
        dados.id_municipio_ocorrencia AS codmun,
        count(*) as obitos30a69
    FROM `basedosdados.br_ms_sim.microdados` AS dados
    LEFT JOIN `dicionario_tipo_obito`
        ON dados.tipo_obito = chave_tipo_obito
    WHERE ano = {ano} and (DATE_DIFF(dados.data_obito, dados.data_nascimento, year) >= 30 and DATE_DIFF(dados.data_obito, dados.data_nascimento, year) <= 69 )
    GROUP BY
    dados.ano,
    -- descricao_tipo_obito,
    dados.id_municipio_ocorrencia
    """




    df_de_prematuros = bd.read_sql(query_de_prematuros, billing_project_id=os.environ['USER'])
    df_de_30_69 = bd.read_sql(query_de_30_69, billing_project_id=os.environ['USER'])

    df = pd.merge(df_de_30_69, df_de_prematuros, how = 'left', on=['codmun', 'ano'])
    if df.shape[0]:
        return df
    return np.array([])


def run_table_saude_obitos_prematuros():
    try:
        mun = get_municipio()
        ultimo_ano = get_ultimo_ano(table_name) + 1
        ano_atual = datetime.now().year + 1
        for ano in range(ultimo_ano, ano_atual):
            df = dataframe(ano)
            if df.shape[0]:
                df = pd.merge(df,mun,on='codmun', how='left')
                add_values(df,table_name)
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")  