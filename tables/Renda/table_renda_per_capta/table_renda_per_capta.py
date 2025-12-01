import pandas as pd
import basedosdados as bd
from utils.utils import get_ultimo_ano, add_values, update_chaves_municipios
import os
from dotenv import load_dotenv

load_dotenv()

# Configurações
TABLE_NAME = 'table_renda_per_capta'
BILLING_PROJECT_ID = os.environ['USER']


# RENDA PER CAPTA É DIFERENTE DE PIB PER CAPTA! CORRIGIR QUANDO HOUVER DADOS DISPONÍVEIS.
def download_and_transform(ano_inicio):
    """
    Busca o PIB per capita municipal no Base dos Dados para preencher a lacuna anual.
    Retorna um DataFrame formatado para inserção.
    """
    # Selecionamos o ID de 6 dígitos para compatibilidade com sua tabela 'municipios_2022'
    # Renomeamos 'pib_per_capita' para 'renda' para seguir o padrão da sua tabela
    query = f"""
    SELECT 
        id_municipio as codmun,
        ano,
        pib as renda
    FROM `basedosdados.br_ibge_pib.municipio`
    WHERE ano >= {ano_inicio}
    """

    print(f"--- Consultando Base dos Dados (BigQuery) para anos > {ano_inicio} ---")
    
    try:
        # O parâmetro billing_project_id é obrigatório para consultas no BigQuery
        df = bd.read_sql(query, billing_project_id=BILLING_PROJECT_ID)
    except Exception as e:
        print(f"Erro ao consultar Base dos Dados: {e}")
        return pd.DataFrame()

    if df.empty:
        return df

    # Garantia de tipagem para evitar erros no banco PostgreSQL
    df['codmun'] = df['codmun'].astype(int)
    df['ano'] = df['ano'].astype(int)
    df['renda'] = df['renda'].astype(float)

    return df

def run_table_renda_per_capta():
    print(f"Iniciando rotina de atualização para: {TABLE_NAME}")
    
    # 1. Verifica o último ano existente no banco para buscar apenas o delta
    try:
        ultimo_ano = get_ultimo_ano(TABLE_NAME)
        print(f"Último ano encontrado no banco: {ultimo_ano}")
    except Exception as e:
        print(f"Não foi possível obter o último ano (Tabela vazia?): {e}")
        ultimo_ano = 0  # Baixa tudo se der erro ou tabela vazia

    # 2. Busca dados novos (Delta)
    #df_new = download_and_transform(ultimo_ano)
    df_new = download_and_transform(2011)
    
    # 3. Insere e Atualiza
    if not df_new.empty:
        print(f"Inserindo {len(df_new)} novos registros...")
        add_values(df_new, TABLE_NAME)
        
        # 4. Atualiza colunas auxiliares (codmun7, nome_sigla) usando a função do utils.py
        # Isso é mais eficiente que fazer merge no Python
        print("Executando update de chaves de municípios...")
        update_chaves_municipios(TABLE_NAME)
        
        print("Atualização concluída com sucesso.")
    else:
        print("Tabela já está atualizada (nenhum dado novo encontrado).")
