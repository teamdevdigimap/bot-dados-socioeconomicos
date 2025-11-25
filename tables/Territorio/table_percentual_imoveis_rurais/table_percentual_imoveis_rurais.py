import basedosdados as bd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from utils.utils import get_municipio, get_ultimo_mes_ano, add_values
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_percentual_imoveis_rurais'


def dataframe(ano, mes):

    query = f"""
    WITH 
    dicionario_situacao_imovel AS (
        SELECT
            chave AS chave_situacao_imovel,
            valor AS descricao_situacao_imovel
        FROM `basedosdados.br_rf_cafir.dicionario`
        WHERE
            TRUE
            AND nome_coluna = 'situacao_imovel'
            AND id_tabela = 'imoveis_rurais'
    )
    SELECT
        dados.id_municipio AS codmun,  -- código do município
        EXTRACT(YEAR FROM dados.data_referencia) AS ano,  -- extrair o ano da data de inscrição
        EXTRACT(MONTH FROM dados.data_referencia) AS mes,  -- extrair o mês da data de inscrição
        SUM(dados.area) AS area_total,  -- somar a área por agrupamento
        municipio.area AS area_municipio  -- área do município da tabela do IBGE
    FROM `basedosdados.br_rf_cafir.imoveis_rurais` AS dados
    LEFT JOIN `dicionario_situacao_imovel`
        ON dados.situacao_imovel = chave_situacao_imovel
    LEFT JOIN `basedosdados.br_ibge_censo_2022.municipio` AS municipio
        ON dados.id_municipio = municipio.id_municipio  -- Correspondência pelo id_municipio
    WHERE EXTRACT(YEAR FROM dados.data_referencia) = {ano} and EXTRACT(MONTH FROM dados.data_referencia) = {mes}
    GROUP BY 
        dados.id_municipio, 
        ano, 
        mes, 
        municipio.area  -- Também deve-se agrupar pela área do município
    ORDER BY 
        ano, 
        mes, 
        codmun;

    """

    df = bd.read_sql(query, billing_project_id=os.environ['USER'])

    df['area_total'] = df['area_total'] / 1e3

    df['percentual'] = df['area_total']/df['area_municipio']*100 
    df = df.dropna()
    df['percentual'] = df['percentual'].apply(lambda x: x if x < 100 else 100)
    df.columns = ['codmun','ano','mes','areatotal','areamunicipio','percentual']
    return df



def run_table_percentual_imoveis_rurais():
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