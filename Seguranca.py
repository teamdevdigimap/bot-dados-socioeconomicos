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
    # try:
    #     table_homicidios.run_table_homicidios()

    #     #update_data_ultima_coleta(rastreabilidade_name,
    #                             #   table_name='table_homicidios')
    #     pass                                
    # except Exception as e:
    #     print(f"Erro na tabela table_homicidios \n{e}")  


    # try:
    #     table_tx_homicidios.run_table_taxa_homicidios()

    #     # update_data_ultima_coleta(rastreabilidade_name,
    #     #                           table_name='table_tx_homicidios')
    #     pass
    # except Exception as e:
    #     print(f"Erro na tabela table_taxa_homicidios \n{e}")  

    # try:
    #     table_tx_mort_violentas_causas_indeterminadas.run_table_taxa_mortes_violentas()

    #     # update_data_ultima_coleta(rastreabilidade_name,
    #     #                           table_name='table_tx_mort_violentas_causas_indeterminadas')
    #     pass
    # except Exception as e:
    #     print(f"Erro na tabela table_tx_mort_violentas_causas_indeterminadas \n{e}")

    # try:
    #     table_tx_obito_acidentes_transito.run_table_taxa_obitos_acidentes()

    #     # update_data_ultima_coleta(rastreabilidade_name,
    #     #                           table_name='table_tx_obito_acidentes_transito')
    #     pass
    # except Exception as e:
    #     print(f"Erro na tabela table_tx_obito_acidentes_transito \n{e}")

    try:
        table_tx_suicidio.run_table_taxa_suicidio()

        # update_data_ultima_coleta(rastreabilidade_name,
        #                           table_name='table_tx_suicidio')
        pass
    except Exception as e:
        print(f"Erro na tabela table_tx_suicidio \n{e}")

    # try:
    #     ## SITE INSTAVEL VERIFICAR ANTES DE RODAR O CÓDIGO
    #     pass
    #     table_censo_suas.run_table_censo_suas()

    #     #update_data_ultima_coleta(rastreabilidade_name, table_name='table_censo_suas')
    # except Exception as e:
    #     print(f"Erro na tabela table_censo_suas \n{e}")
