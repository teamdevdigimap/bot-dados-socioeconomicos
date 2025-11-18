import geopandas as gpd
import os
import requests
import zipfile
from utils.utils import get_table_percentual_terras_indigenas, get_municipio, update_table_percentual_terras_indigenas,add_values
import pandas as pd

table_name = 'table_percentual_terras_indigenas'

local_download = 'tables/Territorio/TerrasIndigenas'

def download_tis_shp():
    try:
        url = "https://geoserver.funai.gov.br/geoserver/Funai/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=Funai%3Atis_poligonais&maxFeatures=10000&outputFormat=SHAPE-ZIP"

        # Diretório onde o script está sendo executado (diretório atual)
        diretorio_atual = local_download

        if not os.path.exists(diretorio_atual):
            # Cria o diretório caso não exista
            os.makedirs(diretorio_atual)

        # Fazendo o download do arquivo
        response = requests.get(url)

        # Verificando se a requisição foi bem-sucedida (código 200)
        if response.status_code == 200:
            # Salvando o conteúdo da resposta em um arquivo ZIP no diretório atual
            arquivo_zip = os.path.join(diretorio_atual, "tis_poligonais.zip")
            with open(arquivo_zip, "wb") as f:
                f.write(response.content)
            print("Download concluído com sucesso!")

            # Descompactando o arquivo ZIP no mesmo diretório
            with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
                zip_ref.extractall(diretorio_atual)
            print(f"Arquivos descompactados em: {diretorio_atual}")
        else:
            print(f"Falha no download. Status code: {response.status_code}")
    except Exception as e:
        print(f'Erro {e}')
        
       
def dataframe():
    tis_poligonais = gpd.read_file(f"{local_download}/tis_poligonais.shp")

    municipios    = gpd.read_file("tables/Territorio/Shp_Municipios/BR_Municipios_2022.shp") 

    municipios.crs = tis_poligonais.crs

    df_interseccao = gpd.overlay(tis_poligonais, municipios, how='intersection')
    df_interseccao = df_interseccao.to_crs(epsg=3395)

    # Verificar a área da interseção
    df_interseccao['area_interseccao_km2'] = df_interseccao.area/10000/100
    df_interseccao

    df_interseccao['percentual'] = df_interseccao['area_interseccao_km2']/df_interseccao['AREA_KM2']*100

    df_interseccao.sort_values(by='terrai_nom')

    df = df_interseccao[['CD_MUN','terrai_cod', 'terrai_nom','area_interseccao_km2', 'AREA_KM2', 'percentual']]

    df.columns = ['codmun','terraicod','terrainome', 'areainterseccaokm2', 'areamunkm2', 'percentual']
    
    return df
 
def df_novo_update(df):
    
    df_banco = get_table_percentual_terras_indigenas(table_name)
    for coluna in df.columns.to_list():
        df_banco[coluna] = df_banco[coluna].astype(df[coluna].dtype)
    
    merged = pd.merge(df_banco, df, on=['codmun', 'nome_sigla','terraicod'], how='right', suffixes=['_banco','_novo'])

    df_novo =  merged[merged['percentual_banco'].isna()]   
    df_novo = df_novo[['codmun','terraicod','terrainome_novo','areainterseccaokm2_novo','areamunkm2_novo','percentual_novo','nome_sigla']]
    df_novo.columns =['codmun','terraicod','terrainome','areainterseccaokm2','areamunkm2','percentual','nome_sigla']
    
    df_update =  merged[~merged['percentual_banco'].isna()]   
    df_update = df_update[df_update['percentual_banco'] != df_update['percentual_novo']]
    df_update = df_update[['codmun','terraicod','terrainome_novo','areainterseccaokm2_novo','areamunkm2_novo','percentual_novo','nome_sigla']]
    df_update.columns =['codmun','terraicod','terrainome','areainterseccaokm2','areamunkm2','percentual','nome_sigla']
    
   
   
    return df_novo, df_update        

    
        
def run_table_percentual_terras_indigenas():
    try:
        download_tis_shp()
        df = dataframe()
        mun = get_municipio()
        df = pd.merge(df,mun,how='left')
        df_novo, df_update = df_novo_update(df)
        
        if df_novo.shape[0]:
            add_values(df_novo,table_name)
        
        if df_update.shape[0]:
            update_table_percentual_terras_indigenas(df_update, table_name)

        
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")    
                