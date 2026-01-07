import pandas as pd
from utils.utils import add_values, update_nome_municipios

def run_ips_data_import(excel_path, ano_referencia):
    """
    Lê o arquivo de Índice de Progresso Social (IPS) e popula as 4 tabelas correspondentes.
    
    Parâmetros:
    excel_path (str): Caminho para o arquivo Excel.
    ano_referencia (int): O ano ao qual os dados se referem (pois não há coluna ano no excel).
    """
    
    print(f"Iniciando processamento do arquivo: {excel_path} para o ano {ano_referencia}")
    
    try:
        # 1. Ler o Excel
        # Assume que o separador decimal no Excel é vírgula, comum em arquivos PT-BR
        df = pd.read_excel(excel_path)
        
        # 2. Normalização dos nomes das colunas (remove espaços extras e quebras de linha)
        df.columns = [c.replace('\n', ' ').strip() for c in df.columns]
        
        # Mapeamento das colunas do Excel para as colunas de valor do Banco
        # Estrutura: 'Nome Excel': 'Nome Coluna Valor Banco'
        map_colunas_valores = {
            'Código IBGE': 'codmun',
            'Índice de Progresso Social': 'ips_indice_progresso_social',
            'Necessidades Humanas Básicas': 'ips_necessidades_humanas_basicas',
            'Fundamentos do Bem-estar': 'ips_fundamentos_bem_estar',
            'Oportunidades': 'ips_oportunidades'
        }
        
        # Renomear as colunas no DataFrame
        df = df.rename(columns=map_colunas_valores)
        
        # 3. Tratamento Geral dos Dados
        
        # Adicionar a coluna de ano
        df['ano'] = int(ano_referencia)
        
        # Garantir que codmun seja inteiro (conforme o CREATE TABLE: codmun int)
        # Se houver sujeira como ponto flutuante ou string, limpamos antes
        df['codmun'] = df['codmun'].astype(str).str.replace('.0', '', regex=False).astype(int)
        
        # Função para limpar e converter decimais (PT-BR -> Float Python)
        def clean_decimal(x):
            if isinstance(x, str):
                return float(x.replace('.', '').replace(',', '.'))
            return float(x)
        
        # Lista de colunas de valor para converter
        cols_valores = [
            'ips_indice_progresso_social', 
            'ips_necessidades_humanas_basicas', 
            'ips_fundamentos_bem_estar', 
            'ips_oportunidades'
        ]
        
        for col in cols_valores:
            if col in df.columns:
                df[col] = df[col].apply(clean_decimal)

        print("Dados carregados e tratados. Iniciando distribuição para tabelas...")

        # 4. Definição das tabelas alvo e suas respectivas colunas de valor
        # Estrutura: ('Nome da Tabela', 'Nome da Coluna de Valor')
        targets = [
            ('table_ips_indice_progresso_social', 'ips_indice_progresso_social'),
            ('table_ips_necessidades_humanas_basicas', 'ips_necessidades_humanas_basicas'),
            ('table_ips_fundamentos_bem_estar', 'ips_fundamentos_bem_estar'),
            ('table_ips_oportunidades', 'ips_oportunidades')
        ]

        # 5. Iterar sobre as tabelas e inserir os dados
        for table_name, value_col in targets:
            print(f"--- Processando tabela: {table_name} ---")
            
            # Criar um sub-dataframe apenas com as colunas necessárias para esta tabela
            # As tabelas pedem: codmun, [coluna_valor], ano
            if value_col in df.columns:
                sub_df = df[['codmun', 'ano', value_col]].copy()
                
                # Inserir dados
                add_values(sub_df, table_name)
                
                # Atualizar metadados (nome_sigla)
                # Nota: A função update_nome_municipios precisa suportar que codmun seja INT
                # Se ela fizer cast explicito (b.codmun::text = m.codmun::text), funcionará bem.
                update_nome_municipios(table_name)
            else:
                print(f"AVISO: Coluna {value_col} não encontrada no Excel. Pulando tabela {table_name}.")

        print("Processo de importação do IPS finalizado com sucesso.")
        
    except Exception as e:
        print(f"Ocorreu um erro crítico durante a importação: {e}")

if __name__ == "__main__":
    # Exemplo de uso
    # Substitua pelo caminho real e o ano correto dos dados
    arquivo_ips = "IPS_Brasil_2024.xlsx" 
    ano_dados = 2025
    
    run_ips_data_import(arquivo_ips, ano_dados)