from tables.Demografia.table_populacao_residente_por_cor_ou_raca import table_populacao_residente_por_cor_ou_raca
from tables.Demografia.table_densidade_domiciliar import table_densidade_domiciliar
from tables.Demografia.table_populacao_censo import table_populacao_censo
from tables.Demografia.table_populacao_estimada import table_populacao_estimada
from tables.Demografia.table_demografia import table_demografia
from tables.Demografia.table_taxa_de_envelhecimento import table_taxa_de_envelhecimento
from tables.Demografia.table_populacao_residente_por_sexo_e_idade import table_populacao_residente_por_sexo_e_idade

##### DEMOGRAFIA #####
def demografia_run():
    try:
        print("\nIniciando table_populacao_residente_por_cor_ou_raca...\n")
        table_populacao_residente_por_cor_ou_raca.run_table_populacao_residente_por_cor_ou_raca()
        print("\nFinalizando table_populacao_residente_por_cor_ou_raca...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_populacao_residente_por_cor_ou_raca \n{e}")

    try:
        print("\nIniciando table_densidade_domiciliar...\n")
        table_densidade_domiciliar.run_table_densidade_domiciliar()
        print("\nFinalizando table_densidade_domiciliar...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_densidade_domiciliar \n{e}")

    try:
        print("\nIniciando table_populacao_censo...\n")
        table_populacao_censo.run_table_populacao_censo()
        print("\nFinalizando table_populacao_censo...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_densidade_domiciliar \n{e}")

    try:
        print("\nIniciando table_populacao_estimada...\n")
        table_populacao_estimada.run_table_populacao_estimada()
        print("\nFinalizando table_populacao_estimada...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_densidade_domiciliar \n{e}")

    try:
        print("\nIniciando table_demografia...\n")
        table_demografia.run_table_demografia()
        print("\nFinalizando table_demografia...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_densidade_domiciliar \n{e}")

    try:
        print("\nIniciando table_taxa_de_envelhecimento...\n")
        table_taxa_de_envelhecimento.run_table_taxa_de_envelhecimento()
        print("\nFinalizando table_taxa_de_envelhecimento...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_densidade_domiciliar \n{e}")

    try:
        print("\nIniciando table_populacao_residente_por_sexo_e_idade...\n")
        table_populacao_residente_por_sexo_e_idade.run_table_populacao_residente_por_sexo_e_idade()
        print("\nFinalizando table_populacao_residente_por_sexo_e_idade...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_densidade_domiciliar \n{e}")
