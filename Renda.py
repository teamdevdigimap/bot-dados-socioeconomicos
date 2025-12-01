from tables.Renda.table_gini import table_gini
from tables.Renda.table_cadunico import table_cadunico
from tables.Renda.table_bpc import table_bpc
from tables.Renda.table_bolsa_familia import table_bolsa_familia
from tables.Renda.table_populacao_ocupada import table_populacao_ocupada
from tables.Renda.table_renda_per_capta import table_renda_per_capta

 ##RENDA###
def renda_run():
#     try:
#         print("\nIniciando table_renda_per_capta...\n")
#         table_renda_per_capta.run_table_renda_per_capta()
#         print("\nFinalizando table_renda_per_capta...\n")
#     except Exception as e:
#         print(f"Erro na tabela table_renda_per_capta \n{e}")
        
    try:
        print("\nIniciando table_gini...\n")
        table_gini.run_table_gini()
        print("\nFinalizando table_gini...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_gini \n{e}")

    try:
        print("\nIniciando table_cadunico...\n")
        table_cadunico.run_table_cadunico()
        print("\nFinalizando table_cadunico...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_cadunico \n{e}")

    try:
        print("\nIniciando table_bpc...\n")
        table_bpc.run_table_bpc()
        print("\nFinalizando table_bpc...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_bpc \n{e}")

    try:
        print("\nIniciando table_bolsa_familia...\n")
        table_bolsa_familia.run_table_bolsa_familia()
        print("\nFinalizando table_bolsa_familia...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_bolsa_familia \n{e}")    

    try:
        print("\nIniciando table_populacao_ocupada...\n")
        table_populacao_ocupada.run_table_populacao_ocupada()
        print("\nFinalizando table_populacao_ocupada...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_populacao_ocupada \n{e}")   
