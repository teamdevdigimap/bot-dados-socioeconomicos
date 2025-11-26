import basedosdados as bd
from datetime import datetime
import pandas as pd
from utils.utils import add_values,get_ultimo_ano, get_municipio
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_homicidios'
ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1
#ultimo_ano = 2019

df_municipios = get_municipio()

def insert_manual_homicidios_from_csv(file_path, sep='\t'):
    """
    Lê um arquivo CSV/TSV contendo dados de homicídios e insere no banco.
    Estrutura esperada do arquivo: codmun, ano, quantidadehomicidiodoloso
    
    Args:
        file_path (str): Caminho completo para o arquivo .csv
        sep (str): Separador do CSV. Padrão definido como tabulação ('\t') com base no exemplo, 
                   mas pode ser alterado para ';' ou ','.
    """
    print(f"\n---> Processando arquivo manual: {file_path} ---")

    try:
        # 1. Leitura do arquivo
        df = pd.read_csv(file_path, sep=sep, dtype={'codmun': str})
        
        # Normalização dos nomes das colunas (remove espaços e poe em minusculo)
        df.columns = df.columns.str.strip().str.lower()
        
        colunas_esperadas = ['codmun', 'ano', 'quantidadehomicidiodoloso']
        
        # Validação básica
        if not all(col in df.columns for col in colunas_esperadas):
             raise Exception(f"O CSV deve conter as colunas: {colunas_esperadas}")

        # 2. Tratamento de dados
        df['quantidadehomicidiodoloso'] = pd.to_numeric(df['quantidadehomicidiodoloso'], errors='coerce').fillna(0).astype(int)
        
        df['ano'] = df['ano'].astype(int)

        # 3. Cruzamento com Tabela de Municípios (Utils)
        global df_municipios
        if df_municipios is None or df_municipios.empty:
            df_municipios = get_municipio()

        # Merge para pegar o nome_sigla
        df_final = df.merge(
            df_municipios, 
            on='codmun', 
            how='left'
        )

        # 4. Preparação final
        df_insert = df_final[['codmun', 'quantidadehomicidiodoloso', 'ano', 'nome_sigla']].copy()
        
        # Filtrar apenas os que tem codmun válido (caso o merge tenha falhado ou csv tenha linha em branco)
        df_insert = df_insert.dropna(subset=['codmun'])
        
        # 5. Inserção
        if not df_insert.empty:
            print(f"Inserindo {len(df_insert)} registros manuais na tabela {table_name}...")
            # print(df_insert.head())
            add_values(df_insert, table_name)
        else:
            print("Nenhum dado válido para inserir (verifique os códigos IBGE).")

    except FileNotFoundError:
        print(f"Erro: O arquivo '{file_path}' não foi encontrado.")
    except Exception as e:
        print(f"Erro crítico ao processar o arquivo manual: {e}")

def dataframe():

    for ano in range(ultimo_ano, ano_atual):
        # print(f"Atualizando ano {ano}")
        query = f"""
        SELECT
            dados.id_municipio AS id_municipio,
            quantidade_homicidio_doloso,
            ano,
        FROM `basedosdados.br_fbsp_absp.municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
            WHERE ano = {ano}
        """

        df = bd.read_sql(query, billing_project_id=os.environ['USER'])
        df = df.rename(columns={'id_municipio':'codmun'})
        df.columns = ['codmun', 'quantidadehomicidiodoloso', 'ano']
        mun = get_municipio()
        df = pd.merge(df,mun, how='left', on='codmun')
        #print("columns>>>>>>>>>>>>", df.columns)
        if df.shape[0]:
            #print(df)
            add_values(df, table_name)
                

def run_table_homicidios():
    try:
        # Executa a rotina automática (Base dos Dados)
        dataframe()
        # Defina o caminho do arquivo aqui quando necessário (quando a base dos dados estiver desatualizada). 
        # Exemplo: caminho_csv = "C:/Users/matheus.souza/Downloads/homicidios.csv"
        caminho_csv = None 
        
        if caminho_csv is not None:
            try:
                if os.path.exists(caminho_csv):
                    # Ajuste o sep conforme seu arquivo (ex: ',' para CSV padrão, '\t' para tabulação)
                    insert_manual_homicidios_from_csv(caminho_csv, sep=';')
                else:
                    print(f"Arquivo CSV manual não encontrado em: {caminho_csv}.")
            except Exception as e:
                print(f"Erro ao inserir dados manuais do CSV: {e}")
        elif caminho_csv is None:
            print("Nenhum arquivo CSV manual fornecido para inserção.")
            
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")