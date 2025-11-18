
#table_populacao_residente_por_sexo_e_idade

import basedosdados as bd
import pandas as pd
from datetime import datetime
from utils.utils import add_values, get_ultimo_ano, get_municipio


table_name = 'table_populacao_residente_por_sexo_e_idade'

def dataframe():
   
    ultimo_ano = get_ultimo_ano(table_name)
    ano_atual = datetime.now().year
    for ano in range(ultimo_ano+1, ano_atual+1):
        print(f"Ano - {ano}")
        if ano:
            query = f"""
                SELECT
                dados.ano as ano,
                dados.id_municipio AS codmun,
                dados.sexo as sexo,
                dados.grupo_idade as grupoidade,
                dados.populacao as populacao
            FROM `basedosdados.br_ms_populacao.municipio` AS dados
            WHERE 
                ano = {ano}
            """

            df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')

            if df.shape[0]:  
                faixa_etaria = {
                    '0-4 anos': '0-19 anos',
                    '5-9 anos': '0-19 anos',
                    '10-14 anos': '0-19 anos',
                    '15-19 anos': '0-19 anos',
                    '20-24 anos': '20-39 anos',
                    '25-29 anos': '20-39 anos',
                    '30-34 anos': '20-39 anos',
                    '35-39 anos': '20-39 anos',
                    '40-44 anos': '40-59 anos',
                    '45-49 anos': '40-59 anos',
                    '50-54 anos': '40-59 anos',
                    '55-59 anos': '40-59 anos',
                    '60-64 anos': '60-79 anos',
                    '65-69 anos': '60-79 anos',
                    '70-74 anos': '60-79 anos',
                    '75-79 anos': '60-79 anos',
                    '80-mais': '80+ anos'
                }


                # Criando uma nova coluna 'faixa_etaria'
                df['grupoidade'] = df['grupoidade'].map(faixa_etaria)

                # Agora podemos agrupar por 'faixa_etaria', 'ano' e 'codmun', somando a população
                df = df.groupby(['ano', 'codmun','sexo' ,'grupoidade'])['populacao'].sum().reset_index()

                df_municipios = get_municipio()
                df = pd.merge(df, df_municipios, on='codmun', how='left')
                if df.shape[0]:
                    #print(df)
                    add_values(df, table_name)

  
def run_table_populacao_residente_por_sexo_e_idade():
    try:
        dataframe()    
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")  