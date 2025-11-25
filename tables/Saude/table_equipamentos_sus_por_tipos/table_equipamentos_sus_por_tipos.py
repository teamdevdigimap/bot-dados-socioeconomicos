import basedosdados as bd
from datetime import datetime
import pandas as pd
import numpy as np
from utils.utils import get_municipio, get_ultimo_ano, add_values, get_ultimo_mes_ano, get_equipamentos_sus_por_tipos, update_table_equipamentos_sus_por_tipos
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_equipamentos_sus_por_tipos'

def get_novo_atualizado(df,ano):
    df['quantidadeequipamentos'] = df['quantidadeequipamentos'].astype(int)
    df_banco = get_equipamentos_sus_por_tipos(ano,table_name)
    df_update = pd.merge(df_banco, df, how='right', suffixes=["_banco","_novo"],on=['codmun','tipoequipamento','nome_sigla','ano'])
    df_update['quantidadeequipamentos_banco'] = df_update['quantidadeequipamentos_banco'].fillna(0).astype(int)
    df_update['mes_banco'] = df_update['mes_banco'].fillna(0).astype(int)
    update = df_update[(df_update['quantidadeequipamentos_banco'] != df_update['quantidadeequipamentos_novo']) & (df_update['quantidadeequipamentos_banco']!= 0)]
    novo = df_update[(df_update['mes_banco'] == 0)]
    novo = novo[['codmun','tipoequipamento','nome_sigla','ano','mes_novo','quantidadeequipamentos_novo']]
    update = update[['codmun','tipoequipamento','nome_sigla','ano','mes_novo','quantidadeequipamentos_novo']]
    novo.columns = ['codmun','tipoequipamento','nome_sigla','ano','mes','quantidadeequipamentos']
    update.columns = ['codmun','tipoequipamento','nome_sigla','ano','mes','quantidadeequipamentos']
    novo = novo[novo['quantidadeequipamentos']>0]
    return novo, update

def dataframe(ano):
    query = f"""
    WITH 
    dicionario_tipo_equipamento AS (
        SELECT
            chave AS chave_tipo_equipamento,
            valor AS descricao_tipo_equipamento
        FROM `basedosdados.br_ms_cnes.dicionario`
        WHERE
            TRUE
            AND nome_coluna = 'tipo_equipamento'
            AND id_tabela = 'equipamento'
    )
    SELECT
        dados.ano as ano,
        MAX(dados.mes) as mes,
        dados.id_municipio AS codmun,
        descricao_tipo_equipamento AS tipoequipamento,
        ROUND(SUM(SAFE_CAST(dados.quantidade_equipamentos AS INT64)) / MAX(dados.mes)) AS quantidadeequipamentos        
        -- ROUND(SUM(SAFE_CAST(dados.quantidade_equipamentos AS INT64))) AS quantidadeequipamentos 
    FROM `basedosdados.br_ms_cnes.equipamento` AS dados
    LEFT JOIN `dicionario_tipo_equipamento`
        ON dados.tipo_equipamento = chave_tipo_equipamento
    WHERE ano = {ano} and descricao_tipo_equipamento is not null
    GROUP BY
        codmun, ano, descricao_tipo_equipamento      
    """

    df = bd.read_sql(query, billing_project_id=os.environ['USER'])

    if df.shape[0]:
        #df = pd.merge(df, mun, on='codmun', how='left')
        return df
    return np.array([])


def run_table_equipamentos_sus_por_tipos():
    try:
        mun  = get_municipio()
        ultimo_ano = get_ultimo_ano(table_name)
        ano_atual = datetime.now().year
        for ano in range(ultimo_ano, ano_atual+1):
            df = dataframe(ano) 
            if df.shape[0]:
                df = pd.merge(df, mun, on='codmun', how='left')
                df_novo,df_update = get_novo_atualizado(df,ano)
                if df_novo.shape[0]:
                    add_values(df_novo,table_name)
                if df_update.shape[0]:
                    update_table_equipamentos_sus_por_tipos(df_update,table_name)  
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")  