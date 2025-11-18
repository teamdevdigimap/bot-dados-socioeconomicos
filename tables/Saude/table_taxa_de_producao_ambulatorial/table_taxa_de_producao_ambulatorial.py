import basedosdados as bd
from datetime import datetime
import pandas as pd
from utils.utils import get_municipio, get_ultimo_ano, add_values


table_name = 'table_taxa_de_producao_ambulatorial'

def dataframe(ano):
    query = f"""
    SELECT
        p.ano,
        p.codmun,
        -- p.populacao,
        -- a.qtdambulatorial,
        (a.qtdambulatorial / p.populacao) AS taxaambulatorial
    FROM 
        (SELECT
            dados.ano as ano,
            dados.id_municipio AS codmun,
            dados.populacao as populacao
        FROM `basedosdados.br_ibge_populacao.municipio` AS dados
        WHERE ano = {ano}) AS p
    JOIN 
        (SELECT
            dados.id_municipio AS codmun,
            SUM(dados.quantidade_produzida_procedimento) as qtdambulatorial,
            dados.ano as ano
        FROM `basedosdados.br_ms_sia.producao_ambulatorial` AS dados
        WHERE ano = {ano}
        GROUP BY ano, codmun) AS a
    ON p.codmun = a.codmun
    """
    df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')
    return df

def run_table_taxa_de_producao_ambulatorial():
    try:
        mun = get_municipio()
        ultimo_ano = get_ultimo_ano(table_name) + 1
        ano_atual = datetime.now().year + 1
        for ano in range(ultimo_ano, ano_atual):
            df = dataframe(ano)
            if df.shape[0]:
                df = pd.merge(df, mun, how='left', on='codmun')
                add_values(df,table_name)
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")  