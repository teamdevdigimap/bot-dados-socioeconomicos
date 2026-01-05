import pandas as pd
from utils.utils import add_values, update_nome_municipios

def run_table_pib_municipal_historico(excel_path):
    """
    Função responsável por ler o Excel histórico do PIB, tratar os dados
    e inserir na tabela 'table_pib_municipal_historico'.
    """
    table_name = 'table_pib_municipal_historico'
    
    print(f"Iniciando processamento do arquivo: {excel_path}")
    
    try:
        # 1. Ler o Excel
        df = pd.read_excel(excel_path)
        
        # 2. Normalização e Mapeamento de colunas
        # Remove quebras de linha e espaços dos nomes das colunas originais
        df.columns = [c.replace('\n', ' ').strip() for c in df.columns]
        
        rename_map = {
            'Código do Município': 'codmun',
            'Ano': 'ano',
            'Produto Interno Bruto, a preços correntes (R$ 1.000)': 'produto_interno_bruto_a_precos_correntes_mil_reais'
        }
        
        # Verifica se as colunas existem antes de renomear
        # Isso ajuda a evitar erros se o layout do Excel mudar ligeiramente
        colunas_existentes = set(df.columns)
        if not set(rename_map.keys()).issubset(colunas_existentes):
            print(f"Erro: As colunas esperadas não foram encontradas no Excel.")
            print(f"Colunas encontradas: {list(df.columns)}")
            return

        df = df.rename(columns=rename_map)
        
        # Selecionar apenas as colunas necessárias
        df = df[['codmun', 'ano', 'produto_interno_bruto_a_precos_correntes_mil_reais']]
        
        # 3. Limpeza e Tipagem dos Dados
        
        # Converter codmun para string e remover '.0' se houver
        df['codmun'] = df['codmun'].astype(str).str.replace('.0', '', regex=False)
        
        # Converter ano para inteiro
        df['ano'] = df['ano'].astype(int)
        
        # Função interna para limpar moeda
        def clean_currency(x):
            if isinstance(x, str):
                # Remove ponto de milhar e troca vírgula decimal por ponto
                return float(x.replace('.', '').replace(',', '.'))
            return float(x)

        df['produto_interno_bruto_a_precos_correntes_mil_reais'] = df['produto_interno_bruto_a_precos_correntes_mil_reais'].apply(clean_currency)

        print("Preview dos dados tratados:")
        print(df.head())
        
        # 4. Inserir no Banco de Dados
        print(f"Inserindo {len(df)} registros na tabela {table_name}...")
        add_values(df, table_name)
        
        # 5. Atualizar nome_sigla
        print("Executando atualização de nome_sigla...")
        update_nome_municipios(table_name)
        
        print("Processo finalizado com sucesso.")
        
    except Exception as e:
        print(f"Ocorreu um erro durante a execução: {e}")

# Bloco de execução local (para testes)
# if __name__ == "__main__":
#     arquivo_excel = "C:/Users/matheus.souza/Downloads/base_de_dados_2010_2023_xlsx/PIB dos Municípios - base de dados 2010-2023.xlsx" 
#     run_table_pib_municipal_historico(arquivo_excel)