import basedosdados as bd
from datetime import datetime
import pandas as pd
import psycopg2
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = 'table_percentual_de_docentes_em_exercicio_i'

ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1

def dataframe():
    for ano in range(ultimo_ano, ano_atual):
        query = f"""
        SELECT
            dados.ano as ano,
            dados.id_municipio AS codmun,
            dados.tipo_classe as faixaensino,
            -- dados.escolaridade as escolaridade,
            -- dados.quantidade_docente as quantidade_docente
            sum(dados.quantidade_docente) as quantidadedocentes
        FROM `basedosdados.br_inep_sinopse_estatistica_educacao_basica.docente_escolaridade` AS dados
        WHERE ano = {ano} and ( dados.escolaridade in ('Graduação - Com Licenciatura', 'Pós Graduação - Doutorado','Pós Graduação - Especialização', 'Pós Graduação - Mestrado'))
        and quantidade_docente > 0
        GROUP BY
            dados.id_municipio,
            dados.ano,
            dados.tipo_classe
        """


        df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')

        mun = get_municipio()

        df = pd.merge(df, mun, on='codmun', how='left')
        if df.shape[0]:
            #print(df)
            add_values(df, table_name)
            

def run_table_percentual_de_docentes_em_exercicio_i():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}") 