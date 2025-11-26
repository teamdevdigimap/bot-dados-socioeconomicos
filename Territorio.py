from tables.Territorio.table_percentual_imoveis_rurais import table_percentual_imoveis_rurais
from tables.Territorio.table_percentual_imoveis_assentamentos import table_percentual_imoveis_assentamentos
from tables.Territorio.table_percentual_terras_indigenas import table_percentual_terras_indigenas
from tables.Territorio.table_percentual_areas_protegidas import table_percentual_areas_protegidas
from tables.Territorio.table_conflitos import table_conflitos
from tables.Territorio.table_percentual_da_area_total_do_municipio import table_percentual_da_area_total_do_municipio
from tables.Territorio.table_numero_de_estabelecimentos_por_tipo_de_exploracao import table_numero_de_estabelecimentos_por_tipo_de_exploracao
from tables.Territorio.table_estabelecimentos_agropecuarios import table_estabelecimentos_agropecuarios

def territorio_run():
    
    # try:
    #     table_percentual_imoveis_rurais.run_table_percentual_imoveis_rurais()
    #     pass
    # except Exception as e:
    #     print(f"Erro na tabela table_percentual_imoveis_rurais \n{e}")  

    # try:
    #     table_percentual_imoveis_assentamentos.run_table_percentual_imoveis_assentamentos()
    #     pass
    # except Exception as e:
    #     print(f"Erro na tabela table_percentual_imoveis_assentamentos \n{e}")
        
        
    try:
        table_percentual_terras_indigenas.run_table_percentual_terras_indigenas()
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_terras_indigenas \n{e}")   
        
    # try:
    #     table_percentual_areas_protegidas.run_table_percentual_areas_protegidas()
    #     pass
    # except Exception as e:
    #     print(f"Erro na tabela table_percentual_areas_protegidas \n{e}")   
        
    # try:
    #     table_conflitos.run_table_conflitos()
    #     pass
    # except Exception as e:
    #     print(f"Erro na tabela table_conflitos \n{e}")  
        
    # try:
    #     table_percentual_da_area_total_do_municipio.run_table_percentual_da_area_total_do_municipio()
    #     pass
    # except Exception as e:
    #     print(f"Erro na tabela table_percentual_da_area_total_do_municipio \n{e}")  


    # try:
    #     table_numero_de_estabelecimentos_por_tipo_de_exploracao.run_table_numero_de_estabelecimentos_por_tipo_de_exploracao()
    #     pass
    # except Exception as e:
    #     print(f"Erro na tabela table_numero_de_estabelecimentos_por_tipo_de_exploracao \n{e}")      
        
    # try:
    #     table_estabelecimentos_agropecuarios.run_table_estabelecimentos_agropecuarios()
    #     pass
    # except Exception as e:
    #     print(f"Erro na tabela table_estabelecimentos_agropecuarios \n{e}")         