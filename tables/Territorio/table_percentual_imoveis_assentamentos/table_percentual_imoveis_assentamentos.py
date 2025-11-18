import os
import pandas as pd
import requests
import zipfile
import geopandas as gpd
from utils.utils import get_municipio, get_table_percentual_imoveis_assentamentos, add_values, update_table_percentual_imoveis_assentamentos

table_name = 'table_percentual_imoveis_assentamentos'

local_download = 'tables/Territorio/Assentamentos'

def download_assentamento_shp():
    try:
        url = "https://storage.googleapis.com/mapbiomas-workspace/TERRITORIES/LULC/BRAZIL/COLLECTION9/WORKSPACE/cat63_settlements_WGS84.zip"

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
            arquivo_zip = os.path.join(diretorio_atual, "assentamento_poligonais.zip")
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
    assentamentos = gpd.read_file(f"{local_download}/cat63_settlements_WGS84.shp")
    municipios    = gpd.read_file("tables/Territorio/Shp_Municipios/BR_Municipios_2022.shp") 
    df_interseccao = gpd.overlay(assentamentos, municipios, how='intersection')
    df_interseccao = df_interseccao.to_crs(epsg=3395)
    df_interseccao['area_km2_assentamentos'] = df_interseccao.area/10000/100
    df_interseccao = df_interseccao.groupby(['CD_MUN', 'AREA_KM2'], as_index=False)['area_km2_assentamentos'].sum()
    df_interseccao['percentual'] = df_interseccao['area_km2_assentamentos']/df_interseccao['AREA_KM2']*100
    df_interseccao = df_interseccao[['CD_MUN','AREA_KM2', 'area_km2_assentamentos', 'percentual']]
    df_interseccao.columns = ['codmun', 'areamunkm2','areaassentamentoskm2','percentual']
    df = df_interseccao
    return df
        

def df_novo_update(df):
    df_banco = get_table_percentual_imoveis_assentamentos(table_name)
    for coluna in df.columns.to_list():
        df_banco[coluna] = df_banco[coluna].astype(df[coluna].dtype)
    
    merged = pd.merge(df_banco, df, on=['codmun', 'nome_sigla'], how='right', suffixes=['_banco','_novo'])

    df_novo =  merged[merged['areaassentamentoskm2_banco'].isna()]   
    df_novo = df_novo[['codmun', 'areamunkm2_novo', 'areaassentamentoskm2_novo', 'percentual_novo', 'nome_sigla']]
    df_novo.columns =['codmun', 'areamunkm2', 'areaassentamentoskm2', 'percentual', 'nome_sigla']
    
    df_update =  merged[~merged['areaassentamentoskm2_banco'].isna()]   
    df_update = df_update[df_update['areaassentamentoskm2_banco'] != df_update['areaassentamentoskm2_novo']]
    df_update = df_update[['codmun', 'areamunkm2_novo', 'areaassentamentoskm2_novo', 'percentual_novo', 'nome_sigla']]
    df_update.columns =['codmun', 'areamunkm2', 'areaassentamentoskm2', 'percentual', 'nome_sigla']
    
    return df_novo, df_update
            
        
def run_table_percentual_imoveis_assentamentos():
    try:
        download_assentamento_shp()
        mun = get_municipio()
        df = dataframe()
        if df.shape[0]:
            df = pd.merge(df,mun,how='left',on='codmun')
            df_novo, df_update = df_novo_update(df)
            if df_novo.shape[0]:
                add_values(df_novo,table_name)
                
            if df_update.shape[0]:
                update_table_percentual_imoveis_assentamentos(df_update,table_name) 
                   
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")             