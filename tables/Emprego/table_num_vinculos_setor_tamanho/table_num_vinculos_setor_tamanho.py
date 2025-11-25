import basedosdados as bd
import pandas as pd
from datetime import datetime
from utils.utils import add_values, get_municipio, get_ultimo_ano
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_num_vinculos_setor_tamanho'

mun = get_municipio()

def dataframe(ano):
    query = f""" 
        WITH 
        dicionario_tamanho_estabelecimento AS (
            SELECT
                chave AS chave_tamanho_estabelecimento,
                valor AS descricao_tamanho_estabelecimento
            FROM `basedosdados.br_me_rais.dicionario`
            WHERE
                TRUE
                AND nome_coluna = 'tamanho_estabelecimento'
                AND id_tabela = 'microdados_estabelecimentos'
        )
        SELECT
            dados.ano AS ano,
            dados.id_municipio AS codmun,
            descricao_tamanho_estabelecimento AS tamanhoestabelecimento,
            -- CAST(LEFT(dados.cnae_1, 2) AS INT64) AS cnae_1,
            CASE
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 1 AND 3 THEN 'Agropecuária, extração vegetal, caça e pesca'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 41 AND 43 THEN 'Construção Civil'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 45 AND 47 THEN 'Comércio'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 49 AND 53 THEN 'Serviços'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 55 AND 56 THEN 'Serviços'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 58 AND 63 THEN 'Serviços'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 64 AND 66 THEN 'Serviços'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) = 68 THEN 'Serviços'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 69 AND 75 THEN 'Serviços'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 77 AND 82 THEN 'Serviços'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 90 AND 93 THEN 'Serviços'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 94 AND 96 THEN 'Serviços'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) = 97 THEN 'Serviços'
                WHEN  CAST(LEFT(dados.cnae_2,5) AS INT64) = 09904 THEN 'Extrativa Mineral'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 10 AND 33 THEN 'Industria de Transformação'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) = 84 THEN 'Administração Publica'
                WHEN CAST(LEFT(dados.cnae_2, 2) AS INT64) BETWEEN 36 AND 38 THEN 'Serviços Industriais de Utilidade Pública'
                WHEN CAST(LEFT(dados.cnae_2, 3) AS INT64) BETWEEN 351 AND 352 THEN 'Serviços Industriais de Utilidade Pública'
                --ELSE 'Outros' 
            END AS setor,
            SUM(dados.quantidade_vinculos_ativos) AS totalvinculos
        FROM `basedosdados.br_me_rais.microdados_estabelecimentos` AS dados
        LEFT JOIN `dicionario_tamanho_estabelecimento`
            ON dados.tamanho_estabelecimento = chave_tamanho_estabelecimento
        WHERE dados.ano = {ano}
        AND dados.quantidade_vinculos_ativos > 0 
        AND descricao_tamanho_estabelecimento <> 'IGNORADO'
        GROUP BY
            dados.ano,
            dados.id_municipio,
            descricao_tamanho_estabelecimento,
            CAST(LEFT(dados.cnae_2, 2) AS INT64),
            setor
        """

    df = bd.read_sql(query, billing_project_id=os.environ['USER'])
    df = df.dropna()
    reclassificacao_dict = {
        '1000 OU MAIS': '1000 OU MAIS',
        'DE 500 A 999': 'DE 100 A 999',
        'DE 250 A 499': 'DE 100 A 999',
        'DE 100 A 249': 'DE 100 A 999',
        'DE 50 A 99': 'DE 50 A 99',
        'DE 20 A 49': 'DE 10 A 49',
        'DE 10 A 19': 'DE 10 A 49',
        'DE 5 A 9': 'ATÉ 9',
        'ATE 4': 'ATÉ 9'
    }
    
    if df.shape[0]:
        df['tamanhoestabelecimento'] = df['tamanhoestabelecimento'].map(reclassificacao_dict)
        df = df.groupby(['ano', 'setor','codmun','tamanhoestabelecimento'])['totalvinculos'].sum().reset_index()
        df = pd.merge(df, mun, on='codmun', how='left')
        
    return df    

def run_table_num_vinculos_setor_tamanho():
    try:
        ultimo_ano = get_ultimo_ano(table_name) + 1
        ano_atual = datetime.now().year + 1
        for ano in range(ultimo_ano, ano_atual):
            df = dataframe(ano)
            if df.shape[0]:
                add_values(df,table_name)
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")     