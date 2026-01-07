import pandas as pd
import numpy as np
from utils.utils import add_values, update_nome_municipios, get_municipio_codmun6

def run_saneamento_import(csv_path, ano_referencia):
    """
    Processa o arquivo CSV de saneamento (água e esgoto), realiza o de-para de
    codmun6 para codmun (7 dígitos) e insere nas respectivas tabelas.
    
    Parâmetros:
    csv_path (str): Caminho para o arquivo CSV.
    ano_referencia (int): Ano dos dados (ex: 2010, 2022).
    """
    
    print(f"Iniciando processamento do arquivo: {csv_path} (Ano: {ano_referencia})")
    
    try:
        # 1. Carregar a tabela de Municípios (do banco)
        # Retorna DataFrame com colunas: ['codmun', 'codmun6', 'nome_sigla']
        df_municipios = get_municipio_codmun6()
        
        # Garantir que as chaves de junção sejam strings limpas
        df_municipios['codmun6'] = df_municipios['codmun6'].astype(str).str.strip()
        df_municipios['codmun'] = df_municipios['codmun'].astype(str).str.strip()
        
        # 2. Ler o CSV de Dados
        # Configuração para ler formato PT-BR (separador vírgula, aspas em strings)
        # Tratamento de valores nulos como '-'
        df_csv = pd.read_csv(
            csv_path, 
            sep=',', 
            quotechar='"', 
            encoding='utf-8', # ou 'latin1' dependendo do arquivo
            dtype={'codmun6': str}, # Ler codmun6 como string para evitar perdas
            na_values=['-', "' -", "'-", ""] # Identificar variações de nulos
        )
        
        # Limpeza do codmun6 do CSV (remover espaços ou aspas residuais)
        df_csv['codmun6'] = df_csv['codmun6'].str.replace("'", "", regex=False).str.strip()
        
        # Função para limpar e converter os valores decimais (ex: "0,153" -> 0.153)
        def clean_float(x):
            if pd.isna(x):
                return None
            if isinstance(x, str):
                # Remove aspas extras se houver, troca vírgula por ponto
                x = x.replace('"', '').replace("'", "").replace(',', '.')
                try:
                    return float(x)
                except ValueError:
                    return None
            return float(x)

        # Colunas de valor para tratar
        cols_valor = [
            'parcela_pop_domicilios_sem_acesso_agua_tratada',
            'parcela_pop_domicilios_sem_coleta_esgoto'
        ]
        
        for col in cols_valor:
            if col in df_csv.columns:
                df_csv[col] = df_csv[col].apply(clean_float)
                
        # 3. Cruzamento (Merge) para obter o codmun de 7 dígitos
        # Apenas linhas que tenham correspondência no banco serão mantidas
        df_merged = pd.merge(df_csv, df_municipios, on='codmun6', how='inner')
        
        print(f"Registros lidos: {len(df_csv)} | Registros mapeados (codmun6->codmun): {len(df_merged)}")
        
        if len(df_merged) == 0:
            print("Nenhum registro foi mapeado. Verifique se os códigos 'codmun6' no CSV batem com a tabela 'municipios_2022'.")
            return

        # Adicionar coluna de ano
        df_merged['ano'] = int(ano_referencia)
        
        # 4. Inserção na Tabela de Água
        table_agua = 'table_populacao_sem_acesso_agua'
        col_agua = 'parcela_pop_domicilios_sem_acesso_agua_tratada'
        
        if col_agua in df_merged.columns:
            # Filtrar apenas colunas necessárias e remover nulos
            df_agua = df_merged[['codmun', col_agua, 'ano']].dropna(subset=[col_agua]).copy()
            
            # Garantir tipos
            df_agua['codmun'] = df_agua['codmun'].astype(int) # Tabela pede int4
            
            if not df_agua.empty:
                print(f"Inserindo {len(df_agua)} registros em {table_agua}...")
                add_values(df_agua, table_agua)
                update_nome_municipios(table_agua)
            else:
                print(f"Sem dados válidos para {table_agua}")

        # 5. Inserção na Tabela de Esgoto
        table_esgoto = 'table_populacao_sem_coleta_esgoto'
        col_esgoto = 'parcela_pop_domicilios_sem_coleta_esgoto'
        
        if col_esgoto in df_merged.columns:
            # Filtrar apenas colunas necessárias e remover nulos
            df_esgoto = df_merged[['codmun', col_esgoto, 'ano']].dropna(subset=[col_esgoto]).copy()
            
            # Garantir tipos
            df_esgoto['codmun'] = df_esgoto['codmun'].astype(int)
            
            if not df_esgoto.empty:
                print(f"Inserindo {len(df_esgoto)} registros em {table_esgoto}...")
                add_values(df_esgoto, table_esgoto)
                update_nome_municipios(table_esgoto)
            else:
                print(f"Sem dados válidos para {table_esgoto}")
                
        print("Processo finalizado.")

    except Exception as e:
        print(f"Erro durante a execução: {e}")




# arquivo_csv = "dados_saneamento.csv" # Substitua pelo caminho real
# ano = 2023 # Substitua pelo ano correto    
# run_saneamento_import(arquivo_csv, ano)