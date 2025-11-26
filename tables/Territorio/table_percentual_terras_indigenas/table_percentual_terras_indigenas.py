import geopandas as gpd
import os
import requests
import zipfile
import pandas as pd
from utils.utils import get_table_percentual_terras_indigenas, get_municipio, update_table_percentual_terras_indigenas, add_values

table_name = 'table_percentual_terras_indigenas'

# Caminhos locais
local_download_tis = 'tables/Territorio/TerrasIndigenas'
local_download_mun = 'tables/Territorio/Shp_Municipios'

def download_tis_shp():
    try:
        url = "https://geoserver.funai.gov.br/geoserver/Funai/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=Funai%3Atis_poligonais&maxFeatures=10000&outputFormat=SHAPE-ZIP"
        
        if not os.path.exists(local_download_tis):
            os.makedirs(local_download_tis)

        # Verifica se já existe para evitar download repetido (opcional)
        if not os.path.exists(os.path.join(local_download_tis, "tis_poligonais.zip")):
            print("Baixando Terras Indígenas...")
            response = requests.get(url)
            if response.status_code == 200:
                arquivo_zip = os.path.join(local_download_tis, "tis_poligonais.zip")
                with open(arquivo_zip, "wb") as f:
                    f.write(response.content)
                
                with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
                    zip_ref.extractall(local_download_tis)
                print("Download TI concluído!")
            else:
                print(f"Falha download TI: {response.status_code}")
                return False
        return True
    except Exception as e:
        print(f'Erro download TI: {e}')
        return False

def download_municipios_shp():
    """Função Nova: Baixa a malha de municípios do IBGE"""
    try:
        # URL oficial do IBGE para malha 2022
        url = "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/BR_Municipios_2022.zip"
        
        if not os.path.exists(local_download_mun):
            os.makedirs(local_download_mun)

        caminho_arquivo_shp = os.path.join(local_download_mun, "BR_Municipios_2022.shp")
        
        # Só baixa se o arquivo .shp final não existir
        if not os.path.exists(caminho_arquivo_shp):
            print("Baixando Municípios IBGE...")
            response = requests.get(url)
            if response.status_code == 200:
                arquivo_zip = os.path.join(local_download_mun, "BR_Municipios_2022.zip")
                with open(arquivo_zip, "wb") as f:
                    f.write(response.content)
                
                with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
                    zip_ref.extractall(local_download_mun)
                print("Download Municípios concluído!")
            else:
                print(f"Falha download Municípios: {response.status_code}")
                return False
        return True
    except Exception as e:
        print(f'Erro download Municípios: {e}')
        return False

def dataframe():
    # Caminhos definidos no topo
    tis_path = f"{local_download_tis}/tis_poligonais.zip" # O geopandas lê direto do zip se quiser, ou do shp extraído
    mun_path = f"{local_download_mun}/BR_Municipios_2022.shp"

    # Verificação de segurança
    if not os.path.exists(mun_path):
        raise FileNotFoundError(f"Arquivo de municípios não encontrado em: {mun_path}")

    tis_poligonais = gpd.read_file(tis_path)
    municipios = gpd.read_file(mun_path) 

    # CORREÇÃO IMPORTANTE: Use to_crs() em vez de atribuir o crs diretamente
    # Atribuir (municipios.crs = ...) só funciona se os dados já estiverem na projeção mas sem metadados.
    # Se estiverem diferentes, você deve converter:
    if municipios.crs != tis_poligonais.crs:
        municipios = municipios.to_crs(tis_poligonais.crs)

    df_interseccao = gpd.overlay(tis_poligonais, municipios, how='intersection')
    df_interseccao = df_interseccao.to_crs(epsg=3395) # Projeção métrica

    df_interseccao['area_interseccao_km2'] = df_interseccao.area/10000/100
    
    # Cálculo do percentual
    # Nota: Certifique-se que AREA_KM2 vem do shapefile do IBGE corretamente
    df_interseccao['percentual'] = df_interseccao['area_interseccao_km2']/df_interseccao['AREA_KM2']*100

    df = df_interseccao[['CD_MUN','terrai_cod', 'terrai_nom','area_interseccao_km2', 'AREA_KM2', 'percentual']]
    df.columns = ['codmun','terraicod','terrainome', 'areainterseccaokm2', 'areamunkm2', 'percentual']
    
    return df

def df_novo_update(df):
    # Mantido idêntico ao original
    df_banco = get_table_percentual_terras_indigenas(table_name)
    for coluna in df.columns.to_list():
        # Verificação defensiva se a coluna existe no banco
        if coluna in df_banco.columns:
            df_banco[coluna] = df_banco[coluna].astype(df[coluna].dtype)
    
    merged = pd.merge(df_banco, df, on=['codmun', 'nome_sigla','terraicod'], how='right', suffixes=['_banco','_novo'])

    df_novo = merged[merged['percentual_banco'].isna()]   
    cols = ['codmun','terraicod','terrainome','areainterseccaokm2','areamunkm2','percentual','nome_sigla']
    
    # Ajuste para pegar colunas com sufixo _novo e renomear
    if not df_novo.empty:
        df_novo = df_novo[['codmun','terraicod','terrainome_novo','areainterseccaokm2_novo','areamunkm2_novo','percentual_novo','nome_sigla']]
        df_novo.columns = cols
    
    df_update = merged[~merged['percentual_banco'].isna()]
    if not df_update.empty:
        df_update = df_update[df_update['percentual_banco'] != df_update['percentual_novo']]
        df_update = df_update[['codmun','terraicod','terrainome_novo','areainterseccaokm2_novo','areamunkm2_novo','percentual_novo','nome_sigla']]
        df_update.columns = cols
    
    return df_novo, df_update        

def run_table_percentual_terras_indigenas():
    try:
        # Executa downloads e verifica sucesso
        ok_tis = download_tis_shp()
        ok_mun = download_municipios_shp()

        if ok_tis and ok_mun:
            df = dataframe()
            mun = get_municipio()
            
            # Merge com tabela auxiliar de municípios (se necessário para pegar nome_sigla, etc)
            df = pd.merge(df, mun, how='left', on='codmun') # Especifiquei 'on' para segurança
            
            df_novo, df_update = df_novo_update(df)
            
            if not df_novo.empty:
                add_values(df_novo, table_name)
                print(f"Inseridos {len(df_novo)} novos registros.")
            
            if not df_update.empty:
                update_table_percentual_terras_indigenas(df_update, table_name)
                print(f"Atualizados {len(df_update)} registros.")
        else:
            print("Erro nos downloads. Processo abortado.")
        
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")