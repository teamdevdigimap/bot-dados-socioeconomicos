from tables.Emprego.table_num_admitidos_desligados import table_num_admitidos_desligados 
from tables.Emprego.table_num_vinculos_setor_tamanho import table_num_vinculos_setor_tamanho 
from tables.Emprego.table_massa_salarial_setor_tamanho import table_massa_salarial_setor_tamanho
from tables.Emprego.table_distribuicao_dos_empregados_formais_por_escolaridade_sexo import table_distribuicao_dos_empregados_formais_por_escolaridade_sexo
from tables.Emprego.table_num_estabelecimento_setor_tamanho import table_num_estabelecimento_setor_tamanho
from tables.Emprego.table_pib_municipal_e_desagregacoes import table_pib_municipal_e_desagregacoes
from tables.Emprego.table_pib_municipal_e_desagregacoes import table_pib_municipal_historico
from tables.Emprego.table_pib_per_capita_municipal import table_pib_per_capita_municipal
from tables.Emprego.table_salario_medio import table_salario_medio

def emprego_run():
    
    try:
        print("\nIniciando table_num_admitidos_desligados...\n")
        table_num_admitidos_desligados.run_table_num_admitidos_desligados()
        print("\nFinalizando table_num_admitidos_desligados...\n")
    except Exception as e:
        print(f"Erro na tabela table_num_admitidos_desligados \n{e}")  
        
        
    try:
        print("\nIniciando table_num_vinculos_setor_tamanho...\n")
        table_num_vinculos_setor_tamanho.run_table_num_vinculos_setor_tamanho()
        print("\nFinalizando table_num_vinculos_setor_tamanho...\n")
    except Exception as e:
        print(f"Erro na tabela table_num_vinculos_setor_tamanho \n{e}")   
        
        
    try:
        print("\nIniciando table_massa_salarial_setor_tamanho...\n")
        table_massa_salarial_setor_tamanho.run_table_massa_salarial_setor_tamanho()
        print("\nFinalizando table_massa_salarial_setor_tamanho...\n")
    except Exception as e:
        print(f"Erro na tabela table_massa_salarial_setor_tamanho \n{e}")           
        
        
    try:
        print("\nIniciando table_num_estabelecimento_setor_tamanho...\n")
        table_num_estabelecimento_setor_tamanho.run_table_num_estabelecimento_setor_tamanho()
        print("\nFinalizando table_num_estabelecimento_setor_tamanho...\n")
    except Exception as e:
        print(f"Erro na tabela table_num_estabelecimento_setor_tamanho \n{e}")      
        
                 
    try:
        print('\nIniciando table_pib_municipal_e_desagregacoes...\n')
        table_pib_municipal_e_desagregacoes.run_table_pib_municipal_e_desagregacoes()
        print('\nFinalizando table_pib_municipal_e_desagregacoes...\n')
    except Exception as e:
        print(f"Erro na tabela table_pib_municipal_e_desagregacoes \n{e}")      
        
    # try:
    #     print('\nIniciando table_pib_municipal_historico...\n')
    #     arquivo_excel = "C:/Users/matheus.souza/Downloads/base_de_dados_2010_2023_xlsx/PIB dos Municípios - base de dados 2010-2023.xlsx" 
    #     table_pib_municipal_historico.run_table_pib_municipal_historico(arquivo_excel)
    #     print('\nFinalizando table_pib_municipal_historico...\n')
    # except Exception as e:
    #     print(f"Erro na tabela table_pib_municipal_historico \n{e}")     

    try:
        print('\nIniciando table_pib_per_capita_municipal...\n')
        table_pib_per_capita_municipal.run_table_pib_per_capita_municipal()
        print('\nFinalizando table_pib_per_capita_municipal...\n')
    except Exception as e:
        print(f"Erro na tabela table_pib_per_capita_municipal \n{e}")     
        
    try:
        print('\nIniciando table_salario_medio...\n')
        table_salario_medio.run_table_salario_medio()
        print('\nFinalizando table_salario_medio...\n')
    except Exception as e:
        print(f"Erro na tabela table_salario_medio \n{e}")           