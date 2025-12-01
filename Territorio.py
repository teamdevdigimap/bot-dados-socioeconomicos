import os
import requests
import zipfile
from tables.Territorio.table_percentual_imoveis_rurais import table_percentual_imoveis_rurais
from tables.Territorio.table_percentual_imoveis_assentamentos import table_percentual_imoveis_assentamentos
from tables.Territorio.table_percentual_terras_indigenas import table_percentual_terras_indigenas
from tables.Territorio.table_percentual_areas_protegidas import table_percentual_areas_protegidas
from tables.Territorio.table_conflitos import table_conflitos
from tables.Territorio.table_percentual_da_area_total_do_municipio import table_percentual_da_area_total_do_municipio
from tables.Territorio.table_numero_de_estabelecimentos_por_tipo_de_exploracao import table_numero_de_estabelecimentos_por_tipo_de_exploracao
from tables.Territorio.table_estabelecimentos_agropecuarios import table_estabelecimentos_agropecuarios


# Caminho centralizado para municípios
LOCAL_DOWNLOAD_MUN = 'tables/Territorio/Shp_Municipios'

def download_municipios_shp():
    """
    Baixa a malha de municípios do IBGE.
    Centralizado aqui para ser executado apenas uma vez.
    """
    try:
        # URL oficial do IBGE para malha 2022
        url = "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/BR_Municipios_2022.zip"
        
        if not os.path.exists(LOCAL_DOWNLOAD_MUN):
            os.makedirs(LOCAL_DOWNLOAD_MUN)

        caminho_arquivo_shp = os.path.join(LOCAL_DOWNLOAD_MUN, "BR_Municipios_2022.shp")
        
        # Só baixa se o arquivo .shp final não existir
        if not os.path.exists(caminho_arquivo_shp):
            print("Iniciando download da malha de Municípios IBGE (Centralizado)...")
            response = requests.get(url, stream=True) # stream=True é bom para arquivos grandes
            if response.status_code == 200:
                arquivo_zip = os.path.join(LOCAL_DOWNLOAD_MUN, "BR_Municipios_2022.zip")
                with open(arquivo_zip, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                print("Extraindo municípios...")
                with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
                    zip_ref.extractall(LOCAL_DOWNLOAD_MUN)
                print("Download Municípios concluído com sucesso!")
            else:
                print(f"Falha no download Municípios: Status {response.status_code}")
                return False
        else:
            print("Arquivo de municípios já existe. Pulando download.")
            
        return True
    except Exception as e:
        print(f'Erro crítico ao baixar municípios no script pai: {e}')
        return False

def territorio_run():
    
    try:
        print("\nIniciando table_percentual_imoveis_rurais...\n")
        table_percentual_imoveis_rurais.run_table_percentual_imoveis_rurais()
        print("\nFinalizando table_percentual_imoveis_rurais...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_imoveis_rurais \n{e}")  
    try:
        print("\nIniciando table_percentual_imoveis_rurais...\n")
        table_percentual_imoveis_rurais.run_table_percentual_imoveis_rurais()
        print("\nFinalizando table_percentual_imoveis_rurais...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_imoveis_rurais \n{e}")  

    try:
        print("\nIniciando table_percentual_imoveis_assentamentos...\n")
        table_percentual_imoveis_assentamentos.run_table_percentual_imoveis_assentamentos()
        print("\nFinalizando table_percentual_imoveis_assentamentos...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_imoveis_assentamentos \n{e}")
    
    #repetido, creio que pro caso de ter sido feito o download durante a execucao da tabela anterior
    try:
        print("\nContinuando table_percentual_imoveis_assentamentos...\n")
        table_percentual_imoveis_assentamentos.run_table_percentual_imoveis_assentamentos()
        print("\nFinalizando table_percentual_imoveis_assentamentos... de fato!\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_imoveis_assentamentos \n{e}")
        
        
    try:
        print("\nIniciando table_percentual_terras_indigenas...\n")
        table_percentual_terras_indigenas.run_table_percentual_terras_indigenas()
        print("\nFinalizando table_percentual_terras_indigenas...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_terras_indigenas \n{e}")   
        
    try:
        print("\nIniciando table_percentual_areas_protegidas...\n")
        table_percentual_areas_protegidas.run_table_percentual_areas_protegidas()
        print("\nFinalizando table_percentual_areas_protegidas...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_areas_protegidas \n{e}")   

    #repetido, creio que pro caso de ter sido feito o download durante a execucao da tabela anterior
    try:
        print("\nContinuando table_percentual_areas_protegidas...\n")
        table_percentual_areas_protegidas.run_table_percentual_areas_protegidas()
        print("\nFinalizando table_percentual_areas_protegidas... de fato!\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_areas_protegidas \n{e}")   
        
    try:
        print("\nIniciando table_conflitos...\n")
        table_conflitos.run_table_conflitos()
        print("\nFinalizando table_conflitos...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_conflitos \n{e}")  
    
    #repetido, creio que pro caso de ter sido feito o download durante a execucao da tabela anterior
    try:
        print("\nContinuando table_conflitos...\n")
        table_conflitos.run_table_conflitos()
        print("\nFinalizando table_conflitos... de fato!\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_conflitos \n{e}")  
        
    try:
        print("\nIniciando table_percentual_da_area_total_do_municipio...\n")
        table_percentual_da_area_total_do_municipio.run_table_percentual_da_area_total_do_municipio()
        print("\nFinalizando table_percentual_da_area_total_do_municipio...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_da_area_total_do_municipio \n{e}")  
    
    #repetido, creio que pro caso de ter sido feito o download durante a execucao da tabela anterior
    try:
        print("\nContinuando table_percentual_da_area_total_do_municipio...\n")
        table_percentual_da_area_total_do_municipio.run_table_percentual_da_area_total_do_municipio()
        print("\nFinalizando table_percentual_da_area_total_do_municipio... de fato!\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_da_area_total_do_municipio \n{e}")  


    try:
        print("\nIniciando table_numero_de_estabelecimentos_por_tipo_de_exploracao...\n")
        table_numero_de_estabelecimentos_por_tipo_de_exploracao.run_table_numero_de_estabelecimentos_por_tipo_de_exploracao()
        print("\nFinalizando table_numero_de_estabelecimentos_por_tipo_de_exploracao...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_numero_de_estabelecimentos_por_tipo_de_exploracao \n{e}")      
    
    #repetido, creio que pro caso de ter sido feito o download durante a execucao da tabela anterior
    try:
        print("\nContinuando table_numero_de_estabelecimentos_por_tipo_de_exploracao...\n")
        table_numero_de_estabelecimentos_por_tipo_de_exploracao.run_table_numero_de_estabelecimentos_por_tipo_de_exploracao()
        print("\nFinalizando table_numero_de_estabelecimentos_por_tipo_de_exploracao... de fato!\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_numero_de_estabelecimentos_por_tipo_de_exploracao \n{e}")      
        
    try:
        print("\nIniciando tableestabelecimentos_agropecuarios...\n")
        table_estabelecimentos_agropecuarios.run_table_estabelecimentos_agropecuarios()
        print("\nFinalizando table_estabelecimentos_agropecuarios...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_estabelecimentos_agropecuarios \n{e}")
