import basedosdados as bd
import pandas as pd
from datetime import datetime
from utils.utils import get_ultimo_ano, get_municipio, add_values, get_table_massa_salarial_setor_tamanho,update_table_massa_salarial_setor_tamanho

table_name = 'table_massa_salarial_setor_tamanho'

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
        dados.ano as ano,
        -- dados.mes as mes,
        MAX(dados.mes) OVER (PARTITION BY dados.id_municipio) as mes, 
        dados.id_municipio AS codmun,
        -- dados.cnae_2_subclasse as cnae_2,
        -- dados.salario_mensal as salario_mensal,
        descricao_tamanho_estabelecimento AS tamanhoestabelecimento,

        CASE
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 1 AND 3 THEN 'Agropecuária, extração vegetal, caça e pesca'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 41 AND 43 THEN 'Construção Civil'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 45 AND 47 THEN 'Comércio'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 49 AND 53 THEN 'Serviços'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 55 AND 56 THEN 'Serviços'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 58 AND 63 THEN 'Serviços'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 64 AND 66 THEN 'Serviços'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) = 68 THEN 'Serviços'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 69 AND 75 THEN 'Serviços'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 77 AND 82 THEN 'Serviços'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 90 AND 93 THEN 'Serviços'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 94 AND 96 THEN 'Serviços'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) = 97 THEN 'Serviços'
            WHEN  CAST(LEFT(dados.cnae_2_subclasse,5) AS INT64) = 09904 THEN 'Extrativa Mineral'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 10 AND 33 THEN 'Industria de Transformação'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) = 84 THEN 'Administração Publica'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 2) AS INT64) BETWEEN 36 AND 38 THEN 'Serviços Industriais de Utilidade Pública'
            WHEN CAST(LEFT(dados.cnae_2_subclasse, 3) AS INT64) BETWEEN 351 AND 352 THEN 'Serviços Industriais de Utilidade Pública'
            --ELSE 'Outros' 
        END AS setor,
        SUM(dados.salario_mensal) as total,
        SUM(dados.salario_mensal) / COUNT(*) AS media,
        count(*) AS quantidade
    FROM `basedosdados.br_me_caged.microdados_movimentacao` AS dados

    LEFT JOIN `dicionario_tamanho_estabelecimento`
        ON dados.tamanho_estabelecimento_janeiro = chave_tamanho_estabelecimento

    WHERE descricao_tamanho_estabelecimento <> 'ZERO' and ano = {ano}

    GROUP BY
        dados.ano,
        dados.mes,
        dados.id_municipio,
        descricao_tamanho_estabelecimento,
        setor

    """

    df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')
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
        df = df.groupby(['ano','mes', 'setor','codmun','tamanhoestabelecimento'])['total','quantidade'].sum().reset_index()
        df['media'] = df['total']/df['quantidade']
        df = df.drop(columns='quantidade')
        df = pd.merge(df, mun, on='codmun', how='left')
        return df
    
    return pd.DataFrame(columns=['vazio'])


def get_novo_and_update(df_novo,df_database):
    merged = pd.merge(df_novo, df_database, on=['codmun','setor','ano', 'tamanhoestabelecimento'], how='left', suffixes=('_novo','_banco'))
    df_novo =  merged[(merged['media_banco'].isna()) & (merged['total_novo'] > 0)]
    df_novo = df_novo[['codmun','setor','tamanhoestabelecimento','total_novo','media_novo','nome_sigla_novo','ano','mes_novo']]
    df_novo.columns = ['codmun','setor','tamanhoestabelecimento','total','media','nome_sigla','ano','mes']

    df_update = merged.dropna(subset='mes_banco')
    df_update['mes_banco'] = df_update['mes_banco'].astype(int)
    df_update = df_update[(df_update['mes_banco'] != df_update['mes_novo']) & (df_update['mes_novo'] != df_update['mes_banco'])]
    df_update = df_update[['codmun','setor','tamanhoestabelecimento','total_novo','media_novo','nome_sigla_novo','ano','mes_novo']]
    df_update.columns = ['codmun','setor','tamanhoestabelecimento','total','media','nome_sigla','ano','mes']
    
    return df_novo, df_update
            
        
def run_table_massa_salarial_setor_tamanho():
    ano_atual = datetime.now().year
    ultimo_ano = get_ultimo_ano(table_name)
    # df_novo = dataframe(2024) 
    for ano in range(ultimo_ano, ano_atual+1):
        df_novo = dataframe(ano)
        if df_novo.shape[0]:
            df_banco = get_table_massa_salarial_setor_tamanho(ano,table_name)
            df_novo, df_update = get_novo_and_update(df_novo,df_banco)
            if df_novo.shape[0]:
                add_values(df_novo,table_name)
            if df_update.shape[0]:
                update_table_massa_salarial_setor_tamanho(df_update,table_name)

    
