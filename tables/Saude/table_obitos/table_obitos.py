import basedosdados as bd
from datetime import datetime
import pandas as pd
import numpy as np
from utils.utils import get_municipio, get_ultimo_ano, add_values

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
    df = bd.read_sql(query = query, billing_project_id = 'fair-kingdom-372516')
    df = df.fillna(0)

    if df.shape[0]:    
        return df   
    return np.array([])


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