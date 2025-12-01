from utils.utils import update_data_ultima_coleta
from tables.Seguranca.table_homicidios import table_homicidios
from tables.Seguranca.table_tx_homicidios import table_tx_homicidios
from tables.Seguranca.table_tx_mort_violentas_causas_indeterminadas import table_tx_mort_violentas_causas_indeterminadas
from tables.Seguranca.table_tx_obito_acidentes_transito import table_tx_obito_acidentes_transito
from tables.Seguranca.table_tx_suicidio import table_tx_suicidio
from tables.Seguranca.table_censo_suas import table_censo_suas

##### SEGURANÇA #####
rastreabilidade_name = 'rastreabilidade_seguranca_publica_teste'
def seguranca_run():
    try:
        print("\nIniciando table_homicidios...\n")
        table_homicidios.run_table_homicidios()
        print("\nFinalizando table_homicidios...\n")

        #update_data_ultima_coleta(rastreabilidade_name,
                                #   table_name='table_homicidios')
        pass                                
    except Exception as e:
        print(f"Erro na tabela table_homicidios \n{e}")  


    try:
        print("\nIniciando table_taxa_homicidios...\n")
        table_tx_homicidios.run_table_taxa_homicidios()
        print("\nFinalizando table_taxa_homicidios...\n")

        # update_data_ultima_coleta(rastreabilidade_name,
        #                           table_name='table_tx_homicidios')
        pass
    except Exception as e:
        print(f"Erro na tabela table_taxa_homicidios \n{e}")  

    try:
        print("\nIniciando table_tx_mort_violentas_causas_indeterminadas...\n")
        table_tx_mort_violentas_causas_indeterminadas.run_table_taxa_mortes_violentas()
        print("\nFinalizando table_tx_mort_violentas_causas_indeterminadas...\n")

        # update_data_ultima_coleta(rastreabilidade_name,
        #                           table_name='table_tx_mort_violentas_causas_indeterminadas')
        pass
    except Exception as e:
        print(f"Erro na tabela table_tx_mort_violentas_causas_indeterminadas \n{e}")

    try:
        print("\nIniciando table_tx_obito_acidentes_transito...\n")
        table_tx_obito_acidentes_transito.run_table_taxa_obitos_acidentes()
        print("\nFinalizando table_tx_obito_acidentes_transito...\n")

        # update_data_ultima_coleta(rastreabilidade_name,
        #                           table_name='table_tx_obito_acidentes_transito')
        pass
    except Exception as e:
        print(f"Erro na tabela table_tx_obito_acidentes_transito \n{e}")

    try:
        print("\nIniciando table_tx_suicidio...\n")
        table_tx_suicidio.run_table_taxa_suicidio()
        print("\nFinalizando table_tx_suicidio...\n")

        # update_data_ultima_coleta(rastreabilidade_name,
        #                           table_name='table_tx_suicidio')
        pass
    except Exception as e:
        print(f"Erro na tabela table_tx_suicidio \n{e}")

    try:
        ## SITE INSTAVEL VERIFICAR ANTES DE RODAR O CÓDIGO
        pass
        print("\nIniciando table_censo_suas...\n")
        table_censo_suas.run_table_censo_suas()
        print("\nFinalizando table_censo_suas...\n")

        #update_data_ultima_coleta(rastreabilidade_name, table_name='table_censo_suas')
    except Exception as e:
        print(f"Erro na tabela table_censo_suas \n{e}")
