import pandas as pd
from utils.utils import add_values, update_nome_municipios

def run_indicador_crianca_alfabetizada(excel_path):
    """
    Lê o Excel de Alfabetização, trata os números decimais (PT-BR)
    e insere na tabela 'table_indicador_crianca_alfabetizada'.
    """
    table_name = 'table_indicador_crianca_alfabetizada'
    
    print(f"Iniciando processamento do arquivo: {excel_path}")
    
    try:
        # 1. Ler o Excel
        df = pd.read_excel(excel_path)
        
        # 2. Normalização dos nomes das colunas
        # Remove espaços e quebras de linha para evitar erros de chave
        df.columns = [c.strip().replace('\n', '') for c in df.columns]
        
        # 3. Tratamento de Tipos de Dados
        
        # Função para converter decimais com vírgula (ex: "64,6" -> 64.6)
        def clean_float(x):
            if pd.isna(x):
                return None
            if isinstance(x, str):
                return float(x.replace('.', '').replace(',', '.'))
            return float(x)

        # Lista de colunas que são FLOAT no banco e podem vir com vírgula no Excel
        cols_float = [
            'percent_alunos_alfabetizados_2023_1_semestre',
            'percent_alunos_alfabetizados_2024_1_semestre',
            'meta_2024_2_semestre',
            'meta_2025',
            'meta_2026',
            'meta_2027',
            'meta_2028',
            'meta_2029',
            'meta_2030',
            'percent_participacao'
        ]

        # Aplicar a conversão de float
        for col in cols_float:
            if col in df.columns:
                df[col] = df[col].apply(clean_float)

        # Tratamento de Inteiros (codmun, ano, nivel_albetizacao)
        if 'codmun' in df.columns:
            # Garante que não haja '.0' se o Excel leu como float, depois converte para int
            df['codmun'] = df['codmun'].astype(str).str.replace('.0', '', regex=False).astype(int)

        if 'ano' in df.columns:
            df['ano'] = df['ano'].astype(int)
            
        # Nota: Mantendo a grafia 'albetizacao' conforme o CREATE TABLE fornecido
        if 'nivel_albetizacao' in df.columns:
            df['nivel_albetizacao'] = df['nivel_albetizacao'].astype(int)

        # 4. Seleção de Colunas para o Banco
        # Filtra apenas as colunas que existem na definição da tabela para evitar erros
        db_cols = [
            'codmun', 
            'ano', 
            'nivel_albetizacao'
        ] + cols_float
        
        # Verifica quais colunas do db_cols estão de fato no dataframe
        cols_to_insert = [c for c in db_cols if c in df.columns]
        
        df_final = df[cols_to_insert].copy()

        # 5. Inserção no Banco
        print(f"Inserindo {len(df_final)} registros na tabela {table_name}...")
        add_values(df_final, table_name)
        
        # 6. Atualização de Metadados (Nome/Sigla do Município)
        print("Atualizando nome e sigla dos municípios...")
        update_nome_municipios(table_name)
        
        print("Processo finalizado com sucesso.")
        
    except Exception as e:
        print(f"Erro ao processar indicador de alfabetização: {e}")


arquivo_excel = "Alfabetizacao_Criancas.xlsx" 
run_indicador_crianca_alfabetizada(arquivo_excel)