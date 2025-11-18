import basedosdados as bd
from datetime import datetime
import pandas as pd
import psycopg2
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = 'table_taxa_de_distorcao_idade_serie'

ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1

def dataframe():
    for ano in range(ultimo_ano, ano_atual):
        query = f"""
        SELECT
            dados.id_municipio AS id_municipio,
            localizacao,
            rede,
            tdi_ef,
            tdi_ef_anos_iniciais,
            tdi_ef_anos_finais,
            tdi_ef_1_ano,
            tdi_ef_2_ano,
            tdi_ef_3_ano,
            tdi_ef_4_ano,
            tdi_ef_5_ano,
            tdi_ef_6_ano,
            tdi_ef_7_ano,
            tdi_ef_8_ano,
            tdi_ef_9_ano,
            tdi_em,
            tdi_em_1_ano,
            tdi_em_2_ano,
            tdi_em_3_ano,
            tdi_em_4_ano,
            ano
        FROM `basedosdados.br_inep_indicadores_educacionais.municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio, nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        WHERE ano = {ano}
        """

        # Carregar os dados do banco de dados
        df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')

        # Dicionário correto para renomear as colunas
        renomear_colunas = {
            'id_municipio': 'codmun',
            'localizacao': 'localizacao',
            'rede': 'dependenciaadministrativa',
            'tdi_ef': 'ensinofundamentaltotal',
            'tdi_ef_anos_iniciais': 'ensinofundamentalanosiniciais',
            'tdi_ef_anos_finais': 'ensinofundamentalanosfinais',
            'tdi_ef_1_ano': 'ensinofundamental1ano',
            'tdi_ef_2_ano': 'ensinofundamental2ano',
            'tdi_ef_3_ano': 'ensinofundamental3ano',
            'tdi_ef_4_ano': 'ensinofundamental4ano',
            'tdi_ef_5_ano': 'ensinofundamental5ano',
            'tdi_ef_6_ano': 'ensinofundamental6ano',
            'tdi_ef_7_ano': 'ensinofundamental7ano',
            'tdi_ef_8_ano': 'ensinofundamental8ano',
            'tdi_ef_9_ano': 'ensinofundamental9ano',
            'tdi_em': 'ensinomediototal',
            'tdi_em_1_ano': 'ensinomedio1serie',
            'tdi_em_2_ano': 'ensinomedio2serie',
            'tdi_em_3_ano': 'ensinomedio3serie',
            'tdi_em_4_ano': 'ensinomedio4serie',
            'ano': 'ano'
        }

        # Renomear as colunas do DataFrame
        df = df.rename(columns=renomear_colunas)
        mun = get_municipio()

        df = pd.merge(df, mun, on='codmun', how='left')
        if df.shape[0]:
            #print(df)
            add_values(df, table_name)

def run_table_taxa_de_distorcao_idade_serie():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}") 