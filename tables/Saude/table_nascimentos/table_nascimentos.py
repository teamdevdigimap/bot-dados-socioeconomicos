import basedosdados as bd
from datetime import datetime
import pandas as pd
from utils.utils import add_values,get_municipio, get_ultimo_ano
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_nascimentos'


def dataframe(ano):

  query = f"""
    SELECT
      dados.ano as ano,
      dados.id_municipio_nascimento AS codmun,
      count(dados.id_municipio_nascimento) as nascidos
  FROM `basedosdados.br_ms_sinasc.microdados` AS dados
  LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio_nascimento
      ON dados.id_municipio_nascimento = diretorio_id_municipio_nascimento.id_municipio
      WHERE ano = {ano}
      GROUP BY ano, codmun 
  """

  df = bd.read_sql(query, billing_project_id=os.environ['USER'])
  return df

def run_table_nascimentos():
    try:
        mun = get_municipio()
        ultimo_ano = get_ultimo_ano(table_name) + 1
        ano_atual = datetime.now().year + 1
        for ano in range(ultimo_ano, ano_atual):
            df = dataframe(ano)
            if df.shape[0]:
                df = pd.merge(df,mun,on='codmun', how='left')
                add_values(df,table_name)
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")      