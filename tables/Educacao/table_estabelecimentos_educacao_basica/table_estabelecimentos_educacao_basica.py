import basedosdados as bd
from datetime import datetime
import pandas as pd
from utils.utils import add_values, get_ultimo_ano, get_municipio
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_estabelecimentos_educacao_basica'

ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1

mun = get_municipio()

def dataframe():
    for ano in range(ultimo_ano, ano_atual):
            
        query = f"""
        SELECT
            ano,
            dados.id_municipio AS codmun,
            SUM(educacao_basica) as `Educação Básica`,
            SUM(etapa_ensino_infantil) as `Ensino Infantil `,
            SUM(etapa_ensino_infantil_creche) as `Ensino Infantil creche`,
            SUM(etapa_ensino_infantil_pre_escola) as `Ensino Infantil pre escola`,
            SUM(etapa_ensino_fundamental) as `Ensino Fundamental`,
            SUM(etapa_ensino_fundamental_anos_iniciais) as `Ensino Fundamental anos iniciais`,
            SUM(etapa_ensino_fundamental_anos_finais) as `Ensino Fundamental anos finais`,
            SUM(etapa_ensino_medio) as `Ensino Médio`,
            SUM(etapa_ensino_profissional) as `Ensino Profissional`,
            SUM(etapa_ensino_eja) as `Ensino EJA`,
            SUM(etapa_ensino_especial) as `Ensino Especial`
        FROM `basedosdados.br_inep_censo_escolar.escola` AS dados
        WHERE 
            ano = {ano}
        GROUP BY 
            ano,
            dados.id_municipio         
        """

        df = bd.read_sql(query, billing_project_id=os.environ['USER'])
    
        df.fillna(0, inplace=True)
        if df.shape[0]:
            vars = df.columns.to_list()[2:]
            df = df.melt(id_vars=['codmun', 'ano'], value_vars=vars, 
                                var_name='escola', value_name='quantidade')
            df = pd.merge(df, mun, on='codmun', how='left')
            #print(df)
            add_values(df, table_name)

def run_table_estabelecimentos_educacao_basica():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}") 