import basedosdados as bd
import pandas as pd
from datetime import datetime
from utils.utils import add_values, get_ultimo_ano, get_municipio, update_nome_municipios
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_receitas_orcamentarias'
#table_name = 'table_receitas_orcamentarias'
ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1

# print(table_name)

def dataframe():
    try:
        for ano in range(ultimo_ano, ano_atual):

            query = f"""
            SELECT
                dados.id_municipio AS id_municipio,
                estagio, 
                conta as Conta,
                valor as Valor, 
                ano
            FROM `basedosdados.br_me_siconfi.municipio_receitas_orcamentarias` AS dados
            LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
                ON dados.id_municipio = diretorio_id_municipio.id_municipio
                WHERE ano = {ano} 
            """



            df = bd.read_sql(query, billing_project_id=os.environ['USER'])
            df = df.rename(columns={'id_municipio':'codmun', 'estagio': 'Deduções'})
            df.columns = ['codmun', 'deducoes', 'conta', 'valor', 'ano']
            df_municipios = get_municipio()
            df = df.merge(df_municipios, on='codmun', how='left')
            if df.shape[0]:
                #print(df)
                add_values(df,table_name)
    except Exception as e:
            print(f"Erro para atualizar tabela {table_name}\nErro: {e}")  

def processar_dataframe_excel(caminho_arquivo):
    """
    Lê o arquivo Excel, formata os dados conforme o DDL e insere no banco.
    """
    try:
        # 1. Leitura do arquivo Excel
        # Certifique-se de que o nome das colunas no Excel seja: codmun, deducoes, conta, valor
        try:
            df = pd.read_excel(caminho_arquivo, engine='openpyxl')
        except Exception:
            # 2. Se falhar, tenta ler como CSV (comum em arquivos que dão erro de 'zip')
            print("Não é um .xlsx válido. Tentando ler como CSV...")
            df = pd.read_csv(caminho_arquivo, sep=None, engine='python', encoding='utf-8-sig')
        # 2. Limpeza e conversão da coluna 'valor' 
        # (Trata casos onde o número vem como string '1.234,56')
        if df['valor'].dtype == 'object':
            df['valor'] = (
                df['valor']
                .astype(str)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
                .astype(float)
            )

        # 3. Adiciona a coluna 'ano' com o valor fixo 2024
        df['ano'] = 2024

        # 4. Garante que o codmun seja tratado como texto (conforme DDL)
        df['codmun'] = df['codmun'].astype(str)

        # 5. Seleciona apenas as colunas que existem no DDL (exceto as automáticas)
        # Ordem: codmun, deducoes, conta, valor, ano
        df = df[['codmun', 'deducoes', 'conta', 'valor', 'ano']]

        if not df.empty:
            print(f"Iniciando inserção de {len(df)} linhas na tabela {table_name}...")
            
            # Insere os dados brutos
            add_values(df, table_name)
            
            # Executa a função de UPDATE para preencher nome_sigla
            #get_municipio(table_name)
            update_nome_municipios(table_name)
            
            print("Processo de append e atualização concluído.")
        else:
            print("O arquivo Excel está vazio.")

    except Exception as e:
        print(f"Erro para atualizar tabela {table_name} via Excel\nErro: {e}")

def run_table_receitas_orcamentarias():
    try:
        #datafFrame()
        diretorio_atual = os.path.dirname(os.path.abspath(__file__))
        caminho_excel = os.path.join(diretorio_atual, 'receitas_orcamentarias_2024.xlsx')
        processar_dataframe_excel(caminho_excel)
        # print(f"Diretório atual: {os.getcwd()}")
        # print(f"Arquivos no diretório atual: {os.listdir('.')}")
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")  

