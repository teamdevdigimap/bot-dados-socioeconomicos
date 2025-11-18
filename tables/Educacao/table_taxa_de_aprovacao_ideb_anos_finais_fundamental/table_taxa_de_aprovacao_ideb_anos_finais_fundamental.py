import basedosdados as bd
from datetime import datetime
import pandas as pd
import psycopg2
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = 'table_taxa_de_aprovacao_ideb_anos_finais_fundamental'

ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1

def dataframe():
    for ano in range(ultimo_ano, ano_atual):
        
        query = f"""
        SELECT
            dados.id_municipio AS id_municipio,
            rede,
            ensino,
            taxa_aprovacao,
            indicador_rendimento,
            anos_escolares,
            ano
        FROM `basedosdados.br_inep_ideb.municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        WHERE ano = {ano}  and ensino = 'fundamental'  and anos_escolares = 'finais (6-9)'
        """

        df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')

        rename = {
            'id_municipio': 'codmun',
            'rede': 'Rede',
            'ensino': 'Ensino',
            'taxa_aprovacao': 'Taxa de Aprovação',
            'indicador_rendimento': 'Indicador de Rendimento (P) - Anos Finais',
            'ano': 'Ano'
        }

        df = df.rename(columns=rename)[['codmun','Rede', 'Taxa de Aprovação','Indicador de Rendimento (P) - Anos Finais', 'Ano']]

        df.columns = [
            'codmun',
            'rede',
            'taxadeaprovacao',
            'indicadorderendimento',
            'ano'
        ]

        mun = get_municipio()

        df = pd.merge(df, mun, on='codmun', how='left')
        if df.shape[0]:
            # print(df)
            add_values(df, table_name)

def run_table_taxa_de_aprovacao_ideb_anos_finais_fundamental():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}") 