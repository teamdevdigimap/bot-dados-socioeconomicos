from tables.Social.tables_ips_todas import run_ips_data_import

# Substitua pelo caminho real e o ano correto dos dados
# arquivo_ips = "C:/Users/matheus.souza/Downloads/ips_brasil_municipios_2025.xlsx"
arquivo_ips = None
ano_dados = 2025
    
    
def social_run():
    try:
        print("\nIniciando importação dos dados do Índice de Progresso Social (IPS)...\n")
        run_ips_data_import(arquivo_ips, ano_dados)
        print("\nFinalizando importação dos dados do Índice de Progresso Social (IPS)...\n")
    except Exception as e:
        print(f"Erro na importação dos dados do IPS \n{e}")