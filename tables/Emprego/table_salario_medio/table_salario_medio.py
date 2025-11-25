import pandas as pd
import basedosdados as bd
from datetime import datetime 
from utils.utils import add_values, get_municipio, get_ultimo_ano
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_salario_medio'

def dataframe(ano):
    query = f"""
        SELECT
            id_municipio AS codmun,  
            AVG(valor_remuneracao_media) AS salario_medio,
            ano
        FROM 
            basedosdados.br_me_rais.microdados_vinculos 
        WHERE 
            ano = {ano}
        GROUP BY
            id_municipio,ano           
        """

    df = bd.read_sql(query, billing_project_id=os.environ['USER'])
    
    return df

def run_table_salario_medio():
    try:
        mun = get_municipio()
        ultimo_ano = get_ultimo_ano(table_name)
        ano_atual = datetime.now().year
        for ano in range(ultimo_ano+1, ano_atual+1): 
            df = dataframe(ano)
            if df.shape[0]:
                df = pd.merge(df,mun,on='codmun', how='left')
                df = df.dropna(subset='codmun') # Remove aonde codmun é NaN 
                add_values(df,table_name)

            
        
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")  


