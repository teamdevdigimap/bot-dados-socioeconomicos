import basedosdados as bd
from datetime import datetime
import pandas as pd
import psycopg2
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = 'table_taxas_de_abandono'

ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1
#ultimo_ano = 2019

def dataframe():
    for ano in range(ultimo_ano, ano_atual):
        #print("ano >>>>>>>>>>", ano)
        query = f"""
        SELECT
            dados.id_municipio AS id_municipio,
            localizacao,
            rede,
            taxa_abandono_ef,
            taxa_abandono_ef_anos_iniciais,
            taxa_abandono_ef_anos_finais,
            taxa_abandono_ef_1_ano,
            taxa_abandono_ef_2_ano,
            taxa_abandono_ef_3_ano,
            taxa_abandono_ef_4_ano,
            taxa_abandono_ef_5_ano,
            taxa_abandono_ef_6_ano,
            taxa_abandono_ef_7_ano,
            taxa_abandono_ef_8_ano,
            taxa_abandono_ef_9_ano,
            taxa_abandono_em,
            taxa_abandono_em_1_ano,
            taxa_abandono_em_2_ano,
            taxa_abandono_em_3_ano,
            taxa_abandono_em_4_ano,
            taxa_abandono_em_nao_seriado,
            ano
        FROM `basedosdados.br_inep_indicadores_educacionais.municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        WHERE ano = {ano}
        """

        rename = {
            'id_municipio': 'codmun',
            'localizacao': 'Localização',
            'rede': 'Dependência Administrativa',
            'taxa_abandono_ef': 'Ensino Fundamental - Total',
            'taxa_abandono_ef_anos_iniciais': 'Ensino Fundamental - Anos Iniciais',
            'taxa_abandono_ef_anos_finais': 'Ensino Fundamental - Anos Finais',
            'taxa_abandono_ef_1_ano': 'Ensino Fundamental - 1º Ano',
            'taxa_abandono_ef_2_ano': 'Ensino Fundamental - 2º Ano',
            'taxa_abandono_ef_3_ano': 'Ensino Fundamental - 3º Ano',
            'taxa_abandono_ef_4_ano': 'Ensino Fundamental - 4º Ano',
            'taxa_abandono_ef_5_ano': 'Ensino Fundamental - 5º Ano',
            'taxa_abandono_ef_6_ano': 'Ensino Fundamental - 6º Ano',
            'taxa_abandono_ef_7_ano': 'Ensino Fundamental - 7º Ano',
            'taxa_abandono_ef_8_ano': 'Ensino Fundamental - 8º Ano',
            'taxa_abandono_ef_9_ano': 'Ensino Fundamental - 9º Ano',
            'taxa_abandono_em': 'Ensino Médio - Total',
            'taxa_abandono_em_1_ano': 'Ensino Médio - 1ª Série',
            'taxa_abandono_em_2_ano': 'Ensino Médio - 2ª Série',
            'taxa_abandono_em_3_ano': 'Ensino Médio - 3ª Série',
            'taxa_abandono_em_4_ano': 'Ensino Médio - 4ª Série',
            'taxa_abandono_em_nao_seriado': 'Ensino Médio - Não-Seriado',
            'ano': 'Ano'
        }


        df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')

        df = df.rename(columns=rename)

        # Dicionário para renomear as colunas
        rename_dict = {
            'Ensino Fundamental - Total': 'taxaabandonoensinofundamental',
            'Ensino Fundamental - Anos Iniciais': 'taxaabandonoensinofundamentalanosiniciais',
            'Ensino Fundamental - Anos Finais': 'taxaabandonoensinofundamentalanosfinais',
            'Ensino Fundamental - 1º Ano': 'taxaabandonoensinofundamental1ano',
            'Ensino Fundamental - 2º Ano': 'taxaabandonoensinofundamental2ano',
            'Ensino Fundamental - 3º Ano': 'taxaabandonoensinofundamental3ano',
            'Ensino Fundamental - 4º Ano': 'taxaabandonoensinofundamental4ano',
            'Ensino Fundamental - 5º Ano': 'taxaabandonoensinofundamental5ano',
            'Ensino Fundamental - 6º Ano': 'taxaabandonoensinofundamental6ano',
            'Ensino Fundamental - 7º Ano': 'taxaabandonoensinofundamental7ano',
            'Ensino Fundamental - 8º Ano': 'taxaabandonoensinofundamental8ano',
            'Ensino Fundamental - 9º Ano': 'taxaabandonoensinofundamental9ano',
            'Ensino Médio - Total': 'taxaabandonoensinomedio',
            'Ensino Médio - 1ª Série': 'taxaabandonoensinomedio1ano',
            'Ensino Médio - 2ª Série': 'taxaabandonoensinomedio2ano',
            'Ensino Médio - 3ª Série': 'taxaabandonoensinomedio3ano',
            'Ensino Médio - 4ª Série': 'taxaabandonoensinomedio4ano',
            'Ensino Médio - Não-Seriado': 'taxaabandonoensinomedionaoseriado',
            'Ano': 'ano',
            'codmun': 'codmun',
            'Localização': 'localizacao',
            'Dependência Administrativa': 'dependenciaadministrativa',
        }

        # Renomear as colunas
        df.rename(columns=rename_dict, inplace=True)

        mun = get_municipio()

        df = pd.merge(df, mun, on='codmun', how='left')

        # Reorganizar as colunas na ordem correta
        columns_order = [
            'taxaabandonoensinomedio4ano',
            'taxaabandonoensinomedionaoseriado',
            'ano',
            'taxaabandonoensinofundamental',
            'taxaabandonoensinofundamentalanosiniciais',
            'taxaabandonoensinofundamentalanosfinais',
            'taxaabandonoensinofundamental1ano',
            'taxaabandonoensinofundamental2ano',
            'taxaabandonoensinofundamental3ano',
            'taxaabandonoensinofundamental4ano',
            'taxaabandonoensinofundamental5ano',
            'taxaabandonoensinofundamental6ano',
            'taxaabandonoensinofundamental7ano',
            'taxaabandonoensinofundamental8ano',
            'taxaabandonoensinofundamental9ano',
            'taxaabandonoensinomedio',
            'taxaabandonoensinomedio1ano',
            'taxaabandonoensinomedio2ano',
            'taxaabandonoensinomedio3ano',
            'nome_sigla',
            'localizacao',
            'dependenciaadministrativa',
            'codmun'
        ]

        # Reordenar o DataFrame
        df = df[columns_order]

        # Alterar tipos de dados conforme especificado
        df = df.astype({
            'ano': 'int64',
            'codmun': 'str',
            'localizacao': 'str',
            'dependenciaadministrativa': 'str',
            # Todas as outras colunas como float
        })

        #print("columns>>>>>>>>>>>", df.columns)
        
        if df.shape[0]:
            print(df)
            #add_values(df, table_name)

def run_table_taxas_de_abandono():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}") 