from tables.Habitação.table_populacao_agua_encanada import table_populacao_agua_encanada
from tables.Habitação.table_percentual_agua_encanada_banheiro import table_percentual_agua_encanada_banheiro
from tables.Habitação.table_percentual_populacao_coleta_lixo import table_percentual_populacao_coleta_lixo
from tables.Habitação.table_energia_eletrica import table_energia_eletrica
from tables.Habitação.table_percentual_paredes_nao_alvenaria import table_percentual_paredes_nao_alvenaria

  ##### HABITAÇÂO #####
def habitacao_run():
  
    try:
        table_populacao_agua_encanada.run_table_populacao_agua_encanada()
        pass
    except Exception as e:
        print(f"Erro na tabela table_populacao_agua_encanada \n{e}")  

    try:
        table_percentual_agua_encanada_banheiro.run_table_percentual_agua_encanada_banheiro()
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_agua_encanada_banheiro \n{e}")  


    try:
        table_percentual_populacao_coleta_lixo.run_table_percentual_populacao_coleta_lixo()
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_populacao_coleta_lixo \n{e}") 
        
    
    try:
        table_energia_eletrica.run_table_energia_eletrica()
        pass
    except Exception as e:
        print(f"Erro na tabela table_energia_eletrica \n{e}")  
        
    try:
        table_percentual_paredes_nao_alvenaria.run_table_percentual_paredes_nao_alvenaria()
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_paredes_nao_alvenaria \n{e}")     
        