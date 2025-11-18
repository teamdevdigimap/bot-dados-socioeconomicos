from tables.Renda.table_gini import table_gini
from tables.Renda.table_cadunico import table_cadunico
from tables.Renda.table_bpc import table_bpc
from tables.Renda.table_bolsa_familia import table_bolsa_familia
from tables.Renda.table_populacao_ocupada import table_populacao_ocupada

 ##RENDA###
def renda_run():
   
    try:
        table_gini.run_table_gini()
        pass
    except Exception as e:
        print(f"Erro na tabela table_gini \n{e}")

    try:
        table_cadunico.run_table_cadunico()
        pass
    except Exception as e:
        print(f"Erro na tabela table_cadunico \n{e}")


    try:
        table_bpc.run_table_bpc()
        pass
    except Exception as e:
        print(f"Erro na tabela table_bpc \n{e}")


    try:
        table_bolsa_familia.run_table_bolsa_familia()
        pass
    except Exception as e:
        print(f"Erro na tabela table_bolsa_familia \n{e}")    


    try:
        table_populacao_ocupada.run_table_populacao_ocupada()
        pass
    except Exception as e:
        print(f"Erro na tabela table_populacao_ocupada \n{e}")   