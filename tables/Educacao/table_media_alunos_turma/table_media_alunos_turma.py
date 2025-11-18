import basedosdados as bd
from datetime import datetime
import pandas as pd
import psycopg2
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = 'table_media_alunos_turma'

ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1

def dataframe():
    for ano in range(ultimo_ano, ano_atual):
        query = f"""
        SELECT
            id_municipio,
            localizacao,
            rede,
            atu_ei,
            atu_ei_creche,
            atu_ei_pre_escola,
            atu_ef,
            atu_ef_anos_iniciais,
            atu_ef_anos_finais,
            atu_ef_1_ano,
            atu_ef_2_ano,
            atu_ef_3_ano,
            atu_ef_4_ano,
            atu_ef_5_ano,
            atu_ef_6_ano,
            atu_ef_7_ano,
            atu_ef_8_ano,
            atu_ef_9_ano,
            atu_em,
            atu_em_1_ano,
            atu_em_2_ano,
            atu_em_3_ano,
            atu_em_4_ano,
            atu_em_nao_seriado,
            ano
        FROM `basedosdados.br_inep_indicadores_educacionais.municipio`
        WHERE ano = {ano} 
        """


        df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')

        df.columns = ['codmun', 'localizacao', 'dependenciaadministrativa', 'educacaoinfantiltotal', 'educacaoinfantilcreche', 'educacaoinfantilpreescola', 'ensinofundamentaltotal', 'ensinofundamentalanosiniciais', 'ensinofundamentalanosfinais', 'ensinofundamental1ano', 'ensinofundamental2ano', 'ensinofundamental3ano', 'ensinofundamental4ano', 'ensinofundamental5ano', 'ensinofundamental6ano', 'ensinofundamental7ano', 'ensinofundamental8ano', 'ensinofundamental9ano', 'ensinomediototal', 'ensinomedio1serie', 'ensinomedio2serie', 'ensinomedio3serie', 'ensinomedio4serie', 'ensinomedionaoseriado', 'ano']

        mun = get_municipio()

        df = pd.merge(df, mun, on='codmun', how='left')
        if df.shape[0]:
            #print(df)
            add_values(df, table_name)

def run_table_media_alunos_turma():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}") 