import basedosdados as bd
import pandas as pd
import numpy as np
from datetime import datetime
from utils.utils import add_values, get_ultimo_ano, get_municipio, update_nome_municipios
import os
from dotenv import load_dotenv

load_dotenv()
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

  df = bd.read_sql(query, billing_project_id=os.environ['USER'])
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


# def run_table_pib_per_capita_municipal_historico(file_path):
#     """
#     Função responsável por ler o arquivo histórico, tratar os dados de PIB per capita
#     e inserir na tabela 'table_pib_per_capita_municipal'.
#     """
#     table_name = 'table_pib_per_capita_municipal'
    
#     print(f"Iniciando processamento do arquivo: {file_path}")
    
#     try:
#         # 1. Ler o arquivo
#         # Verifica a extensão para usar o leitor correto (CSV ou Excel)
#         if file_path.endswith('.csv'):
#             df = pd.read_csv(file_path, encoding='utf-8', sep=';', decimal=',') # Ajuste o sep se necessário
#         else:
#             df = pd.read_excel(file_path)
        
#         # 2. Normalização e Mapeamento de colunas
#         # Remove quebras de linha e espaços extras dos nomes das colunas originais
#         df.columns = [c.replace('\n', ' ').strip() for c in df.columns]
        
#         # Mapeamento específico para a tabela de PIB per capita
#         rename_map = {
#             'Código do Município': 'codmun',
#             'Ano': 'ano',
#             'Produto Interno Bruto per capita, a preços correntes (R$ 1,00)': 'pibpercapita'
#         }
        
#         # Verifica se as colunas existem antes de renomear
#         colunas_existentes = set(df.columns)
#         if not set(rename_map.keys()).issubset(colunas_existentes):
#             print(f"Erro: As colunas esperadas não foram encontradas no arquivo.")
#             print(f"Colunas esperadas: {list(rename_map.keys())}")
#             print(f"Colunas encontradas: {list(df.columns)}")
#             return

#         df = df.rename(columns=rename_map)
        
#         # Selecionar apenas as colunas necessárias para esta tabela
#         df = df[['codmun', 'ano', 'pibpercapita']]
        
#         # 3. Limpeza e Tipagem dos Dados
        
#         # Converter codmun para string e remover '.0' se houver
#         df['codmun'] = df['codmun'].astype(str).str.replace('.0', '', regex=False)
        
#         # Converter ano para inteiro
#         df['ano'] = df['ano'].astype(int)
        
#         # Função interna para limpar valores numéricos (moeda brasileira)
#         def clean_currency(x):
#             if pd.isna(x):
#                 return None
#             if isinstance(x, str):
#                 # Remove ponto de milhar e troca vírgula decimal por ponto
#                 # Ex: "10.731,18" -> 10731.18
#                 return float(x.replace('.', '').replace(',', '.'))
#             return float(x)

#         df['pibpercapita'] = df['pibpercapita'].apply(clean_currency)

#         print("Preview dos dados tratados:")
#         print(df.head())
        
#         # 4. Inserir no Banco de Dados
#         print(f"Inserindo {len(df)} registros na tabela {table_name}...")
#         add_values(df, table_name)
        
#         # 5. Atualizar nome_sigla (Metadados do município)
#         print("Executando atualização de nome_sigla...")
#         update_nome_municipios(table_name)
        
#         print("Processo finalizado com sucesso.")
        
#     except Exception as e:
#         print(f"Ocorreu um erro durante a execução: {e}")

# # Bloco de execução local (para testes)
# if __name__ == "__main__":
#     # Ajuste o caminho conforme necessário
#     arquivo_path = "PIB dos Municípios - base de dados 2010-2023.xlsx" 
#     run_table_pib_per_capita_municipal_historico(arquivo_path)