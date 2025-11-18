import basedosdados as bd
import pandas as pd
import numpy as np
from datetime import datetime
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = "table_pib_per_capita_municipal"


def dataframe(ano):

  query=f"""
      SELECT
      p.id_municipio as codmun,
      --p.pib,
      --pop.populacao,
      ROUND(p.pib / pop.populacao,2) AS pibpercapita,
      p.ano,
  FROM
      (SELECT
          dados.id_municipio AS id_municipio,
          pib,
          ano
      FROM
          `basedosdados.br_ibge_pib.municipio` AS dados
      WHERE
          ano = {ano}) AS p
  JOIN
      (SELECT
          dados.ano AS ano,
          dados.id_municipio AS id_municipio,
          dados.populacao AS populacao
      FROM
          `basedosdados.br_ibge_populacao.municipio` AS dados
      WHERE
          ano = {ano}) AS pop
  ON
      p.id_municipio = pop.id_municipio
      AND p.ano = pop.ano;
  """

  df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')
  if df.shape[0]:  
    return df

  return np.array([])


def run_table_pib_per_capita_municipal():
    try:
        mun = get_municipio()
        ultimo_ano = get_ultimo_ano(table_name)
        ano_atual = datetime.now().year
        for ano in range(ultimo_ano+1, ano_atual+1):
            df = dataframe(ano)
            if df.shape[0]:
                df = pd.merge(df,mun,on='codmun', how='left')
                add_values(df,table_name)
        
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")             