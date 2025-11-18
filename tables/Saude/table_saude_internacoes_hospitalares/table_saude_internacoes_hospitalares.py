import basedosdados as bd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from utils.utils import get_ultimo_mes_ano, add_values, get_municipio

table_name = 'table_saude_internacoes_hospitalares'


def dataframe(ano,mes):
    query = f"""
    SELECT
        count(*) as internacoes,
        dados.ano as ano,
        dados.mes as mes,
        dados.id_municipio_estabelecimento_aih AS codmun,
    FROM `basedosdados.br_ms_sih.servicos_profissionais` AS dados
    WHERE ano = {ano} and mes = {mes}
    group by  ano, mes, codmun
    """

    df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')
    if df.shape[0]:
        return df
    return np.array([])

def run_table_saude_internacoes_hospitalares():
    try:
        mun = get_municipio()
        mes,ano = get_ultimo_mes_ano(table_name)
        ultima_data = datetime(ano, mes, 1)
        ultima_data = ultima_data + relativedelta(months=1)
        data_atual = datetime.now() - relativedelta(months=1)
        while ultima_data < data_atual:
            ano = ultima_data.year
            mes = ultima_data.month
            df = dataframe(ano,mes)
            if df.shape[0]:
                df = pd.merge(df, mun, on='codmun', how='left')
                add_values(df,table_name)
            ultima_data = ultima_data + relativedelta(months=1)     
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")      