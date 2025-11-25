import basedosdados as bd
from datetime import datetime
import pandas as pd
import psycopg2
from utils.utils import add_values, get_ultimo_ano, get_municipio
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_inse'

ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1

mun = get_municipio()

def dataframe():
    for ano in range(ultimo_ano, ano_atual):
        query = f"""
        WITH 
        dicionario_rede AS (
            SELECT
                chave AS chave_rede,
                valor AS descricao_rede
            FROM `basedosdados.br_inep_indicador_nivel_socioeconomico.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'rede'
                AND id_tabela = 'municipio'
        ),
        dicionario_tipo_localizacao AS (
            SELECT
                chave AS chave_tipo_localizacao,
                valor AS descricao_tipo_localizacao
            FROM `basedosdados.br_inep_indicador_nivel_socioeconomico.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_localizacao'
                AND id_tabela = 'municipio'
        )
        SELECT
            dados.id_municipio AS id_municipio,
            diretorio_id_municipio.nome AS id_municipio_nome,
            descricao_rede AS rede,
            dados.inse as inse,
            dados.ano as ano,
            descricao_tipo_localizacao AS tipo_localizacao
        FROM `basedosdados.br_inep_indicador_nivel_socioeconomico.municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        LEFT JOIN `dicionario_rede`
            ON dados.rede = chave_rede
        LEFT JOIN `dicionario_tipo_localizacao`
            ON dados.tipo_localizacao = chave_tipo_localizacao
            
            WHERE ano = {ano}
        """
        df = bd.read_sql(query, billing_project_id=os.environ['USER'])
        df = df[~df.applymap(lambda x: x == None).any(axis=1)]

        df = df[['id_municipio', 'inse', 'rede', 'tipo_localizacao', 'ano']]
        df.columns = ['codmun', 'inse', 'rede', 'tipolocalizacao', 'ano']
        df = pd.merge(df, mun, on='codmun', how='left')
        if df.shape[0]:
            add_values(df, table_name)

def run_table_inse():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}") 