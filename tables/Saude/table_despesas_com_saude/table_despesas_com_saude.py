import basedosdados as bd
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from utils.utils import add_values, get_ultimo_mes_ano, get_municipio
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_despesas_com_saude'

def dataframe(ano,mes):
    query = f"""
    SELECT
        dados.ano as ano,
        dados.mes as mes,
        dados.id_municipio_estabelecimento_aih as codmun,
        -- dados.valor_ato_profissional as valor_ato_profissional
        sum(dados.valor_ato_profissional) as valor
    FROM `basedosdados.br_ms_sih.servicos_profissionais` AS dados
    WHERE ano = {ano} and mes = {mes} 
    GROUP BY ano,mes, dados.id_municipio_estabelecimento_aih

            """
    df = bd.read_sql(query, billing_project_id=os.environ['USER'])

    return df

def run_table_despesas_com_saude():
    try:
        mun = get_municipio()
        mes, ano = get_ultimo_mes_ano(table_name)
        ultima_data = datetime(ano, mes, 1)
        ultima_data = ultima_data + relativedelta(months=1)
        data_atual = datetime.now() - relativedelta(months=1)
        while ultima_data < data_atual:
            MES = ultima_data.month
            ANO = ultima_data.year
            df = dataframe(ANO,MES)
            if df.shape[0]:
                df = pd.merge(df, mun, how='left', on='codmun')
                add_values(df,table_name)
            ultima_data = ultima_data + relativedelta(months=1)     
        
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")  