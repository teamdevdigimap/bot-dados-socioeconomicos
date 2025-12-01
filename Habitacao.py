from tables.Habitação.table_populacao_agua_encanada import table_populacao_agua_encanada
from tables.Habitação.table_percentual_agua_encanada_banheiro import table_percentual_agua_encanada_banheiro
from tables.Habitação.table_percentual_populacao_coleta_lixo import table_percentual_populacao_coleta_lixo
from tables.Habitação.table_energia_eletrica import table_energia_eletrica
from tables.Habitação.table_percentual_paredes_nao_alvenaria import table_percentual_paredes_nao_alvenaria

  ##### HABITAÇÂO #####
def habitacao_run():
  
    try:
        print("\nIniciando table_populacao_agua_encanada...\n")
        table_populacao_agua_encanada.run_table_populacao_agua_encanada()
        print("\nFinalizando table_populacao_agua_encanada...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_populacao_agua_encanada \n{e}")  

    try:
        print("\nIniciando table_percentual_agua_encanada_banheiro...\n")
        table_percentual_agua_encanada_banheiro.run_table_percentual_agua_encanada_banheiro()
        print("\nFinalizando table_percentual_agua_encanada_banheiro...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_agua_encanada_banheiro \n{e}")  


    try:
        print("\nIniciando table_percentual_populacao_coleta_lixo...\n")
        table_percentual_populacao_coleta_lixo.run_table_percentual_populacao_coleta_lixo()
        print("\nFinalizando table_percentual_populacao_coleta_lixo...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_populacao_coleta_lixo \n{e}") 
        
    
    try:
        print("\nIniciando table_energia_eletrica...\n")
        table_energia_eletrica.run_table_energia_eletrica()
        print("\nFinalizando table_energia_eletrica...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_energia_eletrica \n{e}")  
        
    try:
        print("\nIniciando table_percentual_paredes_nao_alvenaria...\n")
        table_percentual_paredes_nao_alvenaria.run_table_percentual_paredes_nao_alvenaria()
        print("\nFinalizando table_percentual_paredes_nao_alvenaria...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_paredes_nao_alvenaria \n{e}")     
        