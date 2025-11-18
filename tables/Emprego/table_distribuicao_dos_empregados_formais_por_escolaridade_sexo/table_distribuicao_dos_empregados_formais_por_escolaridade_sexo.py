import pandas as pd
import basedosdados as bd
import pandas as pd
from datetime import datetime
import numpy as np
import traceback
from utils.utils import get_municipio, add_values, get_ultimo_ano

table_name = 'table_distribuicao_dos_empregados_formais_por_escolaridade_sexo_e_faixa_etaria'


def dataframe(ano):
    query_idade = f""" 
        WITH 
        dicionario_tipo_vinculo AS (
            SELECT
                chave AS chave_tipo_vinculo,
                valor AS descricao_tipo_vinculo
            FROM `basedosdados.br_me_rais.dicionario`
            WHERE
                nome_coluna = 'tipo_vinculo'
                AND id_tabela = 'microdados_vinculos'
        ),
        dicionario_grau_instrucao_apos_2005 AS (
            SELECT
                chave AS chave_grau_instrucao_apos_2005,
                valor AS descricao_grau_instrucao_apos_2005
            FROM `basedosdados.br_me_rais.dicionario`
            WHERE
                nome_coluna = 'grau_instrucao_apos_2005'
                AND id_tabela = 'microdados_vinculos'
        ),
        dicionario_sexo AS (
            SELECT
                chave AS chave_sexo,
                valor AS descricao_sexo
            FROM `basedosdados.br_me_rais.dicionario`
            WHERE
                nome_coluna = 'sexo'
                AND id_tabela = 'microdados_vinculos'
        )

        SELECT
            dados.ano AS ano,
            dados.id_municipio AS codmun,
            dados.idade AS idade,
            COUNT(*) AS total
        FROM `basedosdados.br_me_rais.microdados_vinculos` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio, nome FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        WHERE dados.ano = {ano} AND dados.id_municipio IS NOT NULL
        GROUP BY dados.ano, dados.id_municipio, dados.idade
        ORDER BY dados.id_municipio ASC;
    """


    query_sexo = f""" 
        WITH 
        dicionario_tipo_vinculo AS (
            SELECT
                chave AS chave_tipo_vinculo,
                valor AS descricao_tipo_vinculo
            FROM `basedosdados.br_me_rais.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_vinculo'
                AND id_tabela = 'microdados_vinculos'
        ),
        dicionario_grau_instrucao_apos_2005 AS (
            SELECT
                chave AS chave_grau_instrucao_apos_2005,
                valor AS descricao_grau_instrucao_apos_2005
            FROM `basedosdados.br_me_rais.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'grau_instrucao_apos_2005'
                AND id_tabela = 'microdados_vinculos'
        ),
        dicionario_sexo AS (
            SELECT
                chave AS chave_sexo,
                valor AS descricao_sexo
            FROM `basedosdados.br_me_rais.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'sexo'
                AND id_tabela = 'microdados_vinculos'
        )
        SELECT
            dados.ano as ano,
            dados.id_municipio AS codmun,
            descricao_sexo AS sexo,
            count(*) as total
        FROM `basedosdados.br_me_rais.microdados_vinculos` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        LEFT JOIN `dicionario_tipo_vinculo`
            ON dados.tipo_vinculo = chave_tipo_vinculo
        LEFT JOIN `dicionario_grau_instrucao_apos_2005`
            ON dados.grau_instrucao_apos_2005 = chave_grau_instrucao_apos_2005
        LEFT JOIN `dicionario_sexo`
            ON dados.sexo = chave_sexo
        WHERE ano = {ano} and dados.id_municipio is not null
        GROUP BY dados.ano, dados.id_municipio, descricao_sexo
        ORDER BY dados.id_municipio asc
    """

    query_escolaridade  =f"""
        WITH 
        dicionario_tipo_vinculo AS (
            SELECT
                chave AS chave_tipo_vinculo,
                valor AS descricao_tipo_vinculo
            FROM `basedosdados.br_me_rais.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tipo_vinculo'
                AND id_tabela = 'microdados_vinculos'
        ),
        dicionario_grau_instrucao_apos_2005 AS (
            SELECT
                chave AS chave_grau_instrucao_apos_2005,
                valor AS descricao_grau_instrucao_apos_2005
            FROM `basedosdados.br_me_rais.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'grau_instrucao_apos_2005'
                AND id_tabela = 'microdados_vinculos'
        ),
        dicionario_sexo AS (
            SELECT
                chave AS chave_sexo,
                valor AS descricao_sexo
            FROM `basedosdados.br_me_rais.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'sexo'
                AND id_tabela = 'microdados_vinculos'
        )
        SELECT
            dados.ano as ano,
            dados.id_municipio AS codmun,
            descricao_grau_instrucao_apos_2005 AS escolaridade,
            count(*) as total
        FROM `basedosdados.br_me_rais.microdados_vinculos` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        LEFT JOIN `dicionario_tipo_vinculo`
            ON dados.tipo_vinculo = chave_tipo_vinculo
        LEFT JOIN `dicionario_grau_instrucao_apos_2005`
            ON dados.grau_instrucao_apos_2005 = chave_grau_instrucao_apos_2005
        LEFT JOIN `dicionario_sexo`
            ON dados.sexo = chave_sexo
        WHERE ano = {ano} and dados.id_municipio is not null
        GROUP BY dados.ano, dados.id_municipio, descricao_grau_instrucao_apos_2005
        ORDER BY dados.id_municipio asc
    """


    df_escolaridade = bd.read_sql(query_escolaridade, billing_project_id='fair-kingdom-372516')
    df_sexo = bd.read_sql(query_sexo, billing_project_id='fair-kingdom-372516')
    df_idade = bd.read_sql(query_idade, billing_project_id='fair-kingdom-372516')

    if df_escolaridade.shape[0] and df_sexo.shape[0] and df_idade.shape[0]:

        df_escolaridade = df_escolaridade.pivot_table(index=['ano', 'codmun'], columns='escolaridade', values='total', aggfunc='sum', fill_value=0)
        df_escolaridade.reset_index(inplace=True)
        #df_escolaridade = df_escolaridade.drop(columns=['Código não encontrado nos dicionários oficiais.'])
        
        df_escolaridade_vazio = pd.DataFrame(columns=['ano',
                                    'codmun',
                                    'serie5acofund',
                                    'serie6a9fund',
                                    'analfabeto',
                                    'ate5ainc',
                                    'doutorado',
                                    'fundcompl',
                                    'mediocompl',
                                    'medioincomp',
                                    'mestrado',
                                    'supcomp',
                                    'supincomp'])
        
    
        df_escolaridade.columns = df_escolaridade.columns.str.lower()
        
        df_escolaridade = pd.concat([df_escolaridade, df_escolaridade_vazio], ignore_index=True)


        df_sexo = df_sexo.pivot_table(index=['ano', 'codmun'], columns='sexo', values='total', aggfunc='sum', fill_value=0)
        df_sexo.reset_index(inplace=True)
        #df_sexo = df_sexo.drop(columns=['Código não encontrado nos dicionários oficiais.'])
        df_sexo = df_sexo.rename(columns={'Masculino':'masculino', 'Feminino':'feminino'})


        df_idade = df_idade.pivot_table(index=['ano', 'codmun'], columns='idade', values='total', aggfunc='sum', fill_value=0)
        df_idade.reset_index(inplace=True)
        colunas = df_idade.columns[2:]  # Seleciona colunas a partir da coluna 2
        novos_nomes = {col: f'idade{col}' for col in colunas}
        df_idade.rename(columns=novos_nomes, inplace=True)

        df = df_idade.merge(df_sexo, on=['codmun','ano'], how='outer').merge(df_escolaridade, on=['codmun','ano'], how='outer')
        return df
    return np.array([])    


def run_table_distribuicao_dos_empregados_formais_por_escolaridade_sexo():
    try:
        mun = get_municipio()
        ultimo_ano = get_ultimo_ano(table_name)
        ano_atual = datetime.now().year
        for ano in range(ultimo_ano+1, ano_atual+1):
            df = dataframe(ano)
            if df.shape[0]:
                df = pd.merge(df,mun, how='left', on='codmun')
                df.fillna(0, inplace=True)
                add_values(df, table_name) 

    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")   
        
        