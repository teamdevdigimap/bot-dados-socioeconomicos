import basedosdados as bd
from datetime import datetime
import pandas as pd
import numpy as np
from utils.utils import get_municipio, get_ultimo_ano, add_values
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_obitos'

def dataframe(ano):
    query = f"""
    SELECT
        dados.ano as ano,
        dados.id_municipio AS codmun,
        diretorio_causa_basica.descricao_capitulo AS causa,
        -- dados.numero_obitos as numero_obitos,
        sum(dados.numero_obitos) as obitos
    FROM `basedosdados.br_ms_sim.municipio_causa` AS dados
    LEFT JOIN (SELECT DISTINCT subcategoria,descricao_subcategoria,descricao_categoria,descricao_capitulo  FROM `basedosdados.br_bd_diretorios_brasil.cid_10`) AS diretorio_causa_basica
        ON dados.causa_basica = diretorio_causa_basica.subcategoria
    WHERE ano = {ano}
    GROUP BY
        dados.ano,
        dados.id_municipio,
        diretorio_causa_basica.descricao_capitulo
    """
    df = bd.read_sql(query = query, billing_project_id = os.environ['USER'])
    df = df.fillna(0)

    if df.shape[0]:    
        return df   
    return np.array([])


#UTILIZANDO A TABELA DE MICRODADOS QUE ESTÁ MAIS ATUALIZADA, DESCOMENTAR SE NECESSÁRIO RETORNAR A UTILIZAR
# def dataframe(ano):
#     # Usando a tabela de microdados.
#     query = f"""
#     SELECT
#         dados.ano as ano,
#         dados.id_municipio_residencia AS codmun,
#         diretorio_causa_basica.descricao_capitulo AS causa, -- Usando descricao_capitulo como 'causa'
#         COUNT(dados.ano) AS obitos -- Contando as linhas para obter o número de óbitos
#     FROM `basedosdados.br_ms_sim.microdados` AS dados
#     LEFT JOIN (
#         SELECT DISTINCT subcategoria, descricao_capitulo
#         FROM `basedosdados.br_bd_diretorios_brasil.cid_10`
#     ) AS diretorio_causa_basica
#         ON dados.causa_basica = diretorio_causa_basica.subcategoria
#     WHERE ano = {ano}
#     GROUP BY
#         dados.ano,
#         dados.id_municipio_residencia,
#         diretorio_causa_basica.descricao_capitulo
#     HAVING
#         dados.id_municipio_residencia IS NOT NULL -- Garantir que codmun não seja nulo
#     """
    
#     # Restante da lógica (sem alterações)
#     print(f"Buscando dados de óbitos para o ano {ano} na tabela de microdados...")
#     df = bd.read_sql(query = query, billing_project_id = os.environ['USER'])
#     df = df.fillna(0)

#     if df.shape[0]:    
#         return df   
#     return np.array([])


def run_table_obitos():
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