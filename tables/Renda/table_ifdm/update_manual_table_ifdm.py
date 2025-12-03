import pandas as pd
import sys
import os
import re


current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# Tenta importar as funções utilitárias
try:
    from utils.utils import update_chaves_municipios, add_values
except ImportError:
    print("ERRO CRÍTICO: Não foi possível importar 'utils.utils'.")
    print(f"Caminho tentado: {project_root}")
    sys.exit(1)

# --- CONFIGURAÇÃO ---
# caminho do arquivo excel
FILE_PATH = "C:/Users/matheus.souza/Downloads/Serie-Historica-IFDM-2013-a-2023.xlsx"
# nome da tabela no excel
SHEET_NAME = 'IFDM Geral' 
# nome da tabela no banco de dados
TABLE_NAME = 'table_ifdm'

def processar_e_inserir_ifdm(file_path):
    try:
        print(f"1. Lendo arquivo Excel: {file_path} (Aba: {SHEET_NAME})")
        df = pd.read_excel(file_path, sheet_name=SHEET_NAME, header=0)

        cols_interesse = ['COD_MUNIC'] + [col for col in df.columns if str(col).startswith('IFDM 20')]
        df_filtered = df[cols_interesse].copy()

        print("2. Transformando série temporal...")
        df_melted = df_filtered.melt(
            id_vars=['COD_MUNIC'], 
            var_name='ano_str', 
            value_name='ifdm'
        )

        # --- TRATAMENTOS ---
        # Extrair ano
        df_melted['ano'] = df_melted['ano_str'].astype(str).str.extract(r'(\d+)').astype(int)
        
        # Renomear codmun
        df_melted = df_melted.rename(columns={'COD_MUNIC': 'codmun'})
        
        # Tratar valor IFDM (converter string "0,45" para float 0.45 se necessário)
        def limpar_valor(val):
            if isinstance(val, str):
                val = val.replace(',', '.')
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        df_melted['ifdm'] = df_melted['ifdm'].apply(limpar_valor)

        # Limpar nulos
        df_melted = df_melted.dropna(subset=['ifdm', 'codmun'])

        # --- PREPARAÇÃO FINAL ---
        df_final = df_melted[['codmun', 'ifdm', 'ano']].copy()
        
        # Garantir formato de string no codmun (remove .0 do excel)
        df_final['codmun'] = df_final['codmun'].astype(str).str.replace(r'\.0$', '', regex=True)

        # [MODIFICAÇÃO SOLICITADA]: 
        # Cria as colunas como None para a inserção, sem processar chaves agora.
        df_final['codmun7'] = None
        df_final['nome_sigla'] = None

        print(f"3. Inserindo {len(df_final)} registros na tabela {TABLE_NAME} (codmun7/nome_sigla como NULL)...")
        
        # INSERÇÃO NO BANCO
        add_values(df_final, TABLE_NAME)
        
        print("4. Executando atualização de chaves (codmun7 e nome_sigla) no banco...")
        
        # ATUALIZAÇÃO PÓS-INSERÇÃO
        # Executa a função passando o nome da tabela para rodar o UPDATE SQL
        update_chaves_municipios(TABLE_NAME)
        
        print("Processo finalizado com sucesso.")

    except Exception as e:
        print(f"ERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if os.path.exists(FILE_PATH):
        processar_e_inserir_ifdm(FILE_PATH)
    else:
        print(f"Arquivo não encontrado: {FILE_PATH}")