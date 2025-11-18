import basedosdados as bd
import pandas as pd
from datetime import datetime
import numpy as np
from utils.utils import add_values, get_municipio, get_saude_prod_ambula_qtd_aprovada, get_ultimo_ano,update_table_saude_prod_ambula_qtd_aprovada

table_name = 'table_saude_prod_ambula_qtd_aprovada'


def dataframe(ano):
    query = f"""
        SELECT
            dados.id_municipio AS codmun,
            dados.ano AS ano,
            MAX(dados.mes) as mes, 
            SUM(dados.quantidade_aprovada_procedimento) AS quantidade_aprovada_total
        FROM `basedosdados.br_ms_sia.producao_ambulatorial` AS dados
        LEFT JOIN (
            SELECT DISTINCT id_municipio, nome
            FROM `basedosdados.br_bd_diretorios_brasil.municipio`
        ) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        WHERE dados.id_municipio IS NOT NULL and ano = {ano}
        GROUP BY
            dados.id_municipio,
            dados.ano
            --dados.mes
        """

    df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')
    df = df[['codmun', 'quantidade_aprovada_total', 'ano','mes']]
    df.columns = ['codmun', 'quantidadeaprovadatotal', 'ano','mes']
    
    if df.shape[0]:
        return df
    return np.array([])

def get_novo_and_update(df_novo,df_database):
    merged = pd.merge(df_novo, df_database, on=['codmun', 'ano','nome_sigla'], how='left', suffixes=('_novo','_banco'))
    df_novo =  merged[(merged['mes_banco'].isna())]
    df_novo = df_novo[['codmun', 'quantidadeaprovadatotal_novo', 'ano', 'mes_novo', 'nome_sigla']]
    df_novo.columns = ['codmun', 'quantidadeaprovadatotal', 'ano', 'mes', 'nome_sigla']
    df_update = merged[~(merged['mes_banco'].isna())]
    # df_update['mes_banco'] = df_update['mes_banco'].astype(int)
    df_update = df_update[(df_update['mes_banco'] != df_update['mes_novo']) & (df_update['quantidadeaprovadatotal_novo'] != df_update['quantidadeaprovadatotal_banco'])]
    df_update = df_update[['codmun', 'quantidadeaprovadatotal_novo', 'ano', 'mes_novo', 'nome_sigla']]
    df_update.columns = ['codmun', 'quantidadeaprovadatotal', 'ano', 'mes', 'nome_sigla']
    return df_novo, df_update



def run_table_saude_prod_ambula_qtd_aprovada():
    try:
        mun = get_municipio()
        ultimo_ano = get_ultimo_ano(table_name)
        ano_atual = datetime.now().year
        for ano in range(ultimo_ano, ano_atual+1):
            df_banco = get_saude_prod_ambula_qtd_aprovada(ano,table_name)
            df = dataframe(ano)
            
            if df.shape[0]:
                df = pd.merge(df, mun, on='codmun', how='left')
                df_novo, df_update = get_novo_and_update(df,df_banco)
                if df_novo.shape[0]:
                    add_values(df_novo,table_name)
                if df_update.shape[0]:
                    update_table_saude_prod_ambula_qtd_aprovada(df_update,table_name)

    except Exception as e:
        print(f"Erro ao cadastrar os dados na tabela {table_name}\nErro {e}")      
        
        