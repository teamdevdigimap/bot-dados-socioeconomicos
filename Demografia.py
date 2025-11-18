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
        table_populacao_residente_por_cor_ou_raca.run_table_populacao_residente_por_cor_ou_raca()
        pass
    except Exception as e:
        print(f"Erro na tabela table_populacao_residente_por_cor_ou_raca \n{e}")

    try:
        table_densidade_domiciliar.run_table_densidade_domiciliar()
        pass
    except Exception as e:
        print(f"Erro na tabela table_densidade_domiciliar \n{e}")

    try:
        table_populacao_censo.run_table_populacao_censo()
        pass
    except Exception as e:
        print(f"Erro na tabela table_densidade_domiciliar \n{e}")

    try:
        table_populacao_estimada.run_table_populacao_estimada()
        pass
    except Exception as e:
        print(f"Erro na tabela table_densidade_domiciliar \n{e}")

    try:
        table_demografia.run_table_demografia()
        pass
    except Exception as e:
        print(f"Erro na tabela table_densidade_domiciliar \n{e}")

    try:
        table_taxa_de_envelhecimento.run_table_taxa_de_envelhecimento()
        pass
    except Exception as e:
        print(f"Erro na tabela table_densidade_domiciliar \n{e}")

    try:
       
        table_populacao_residente_por_sexo_e_idade.run_table_populacao_residente_por_sexo_e_idade()
        pass
    except Exception as e:
        print(f"Erro na tabela table_densidade_domiciliar \n{e}")
