import zipfile
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import time
import os
import shutil
from glob import glob
import geopandas as gpd
import pandas as pd
from utils.utils import get_municipio, get_table_percentual_areas_protegidas, update_table_percentual_areas_protegidas, add_values

table_name = 'table_percentual_areas_protegidas'

local_download = 'tables/Territorio/AreasProtegidas'

def download_shape():
    # Caminho para o diretório de download desejado
    download_dir = local_download

    # Verificar se o diretório existe, se não, cria-lo
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # Configurar o Selenium para usar o Chrome
    options = webdriver.ChromeOptions()

    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")


    download_dir = f'{os.getcwd()}/{download_dir}'

    # Alterar o diretório de download
    prefs = {
        "download.default_directory": download_dir,  # Definir o diretório de download
        "download.prompt_for_download": False,        # Desabilitar o prompt de download
        "directory_upgrade": True                     # Forçar o Chrome a usar o diretório de download especificado
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)

    

    # Acessar a página
    driver.get("https://cnuc.mma.gov.br/map")

    # Esperar a página carregar
    time.sleep(5)

    # Encontrar o botão de download e clicar
    try:
        # Clicar no botão de download inicial
        download_button = driver.find_element(By.XPATH, "//button[@type='button' and contains(text(),'Download do geo da UC:')]")
        download_button.click()
        
        # Esperar a página carregar e a seleção de formato aparecer
        time.sleep(2)
        
        # Selecionar o input de formato 'shp'
        shp_radio_button = driver.find_element(By.XPATH, "//input[@class='jss4' and @value='shp']")
        shp_radio_button.click()

        # Esperar um pouco para garantir que a opção foi selecionada
        time.sleep(2)
        
        # Clicar no botão de download (agora habilitado)
        download_button_final = driver.find_element(By.XPATH, "//button[@type='button' and contains(text(),'Download') and contains(@class, 'btn-success')]")
        download_button_final.click()

        # Aguardar o tempo necessário para o download ser concluído
        time.sleep(75)

    except Exception as e:
        print(f"Erro ao encontrar o botão de download ou selecionar o formato: {e}")

    # Fechar o navegador
    driver.quit()
    
    arquivo_zip = os.listdir(download_dir)
    arquivo_zip = [i for i in arquivo_zip if ".zip" in i][0]
    
    with zipfile.ZipFile(f'{download_dir}/{arquivo_zip}', 'r') as zip_ref:
        zip_ref.extractall(download_dir)
    print(f"Arquivos descompactados em: {download_dir}")
    
def get_shp_uc():
    download_dir = f"{local_download}/*.shp"
    arquivo = glob(download_dir)
    if arquivo:
        arquivo = arquivo[0]
        df = gpd.read_file(arquivo)
        return df
    return arquivo

def dataframe():
    mun = get_municipio()
    shp = get_shp_uc()
    shp['geometry'] = shp.buffer(0)
    municipios    = gpd.read_file("tables/Territorio/Shp_Municipios/BR_Municipios_2022.shp") 
    municipios.crs = shp.crs
    df_interseccao = gpd.overlay(shp, municipios, how='intersection')
    df_interseccao = df_interseccao.to_crs(epsg=3395)

    # Verificar a área da interseção
    df_interseccao['area_interseccao_km2'] = df_interseccao.area/10000/100
    df_interseccao = df_interseccao.groupby(['CD_MUN', 'AREA_KM2'], as_index=False)['area_interseccao_km2'].sum()
    df_interseccao['percentual'] = df_interseccao['area_interseccao_km2']/df_interseccao['AREA_KM2']*100
    df_interseccao = df_interseccao[['CD_MUN','AREA_KM2', 'area_interseccao_km2', 'percentual']]
    df_interseccao.columns = ['codmun', 'areamunkm2','areasprotegidaskm2','percentual']
    df = df_interseccao
    df = df[df['percentual']< 100]
    
    df = pd.merge(df, mun, on='codmun', how='left')
    
    return df

def get_df_novo_update(df):
    df_banco = get_table_percentual_areas_protegidas(table_name)
    df_banco = df_banco.astype(df.dtypes)

    df_banco['codmun'] = df_banco['codmun'].astype(str)
    df['codmun'] = df['codmun'].astype(str)

    df_merged = pd.merge(df,df_banco, on=['codmun','nome_sigla'],suffixes=('_df1', '_df2'), how='outer')
    df_diff = df_merged

    df_diff = df_diff[((df_diff['areasprotegidaskm2_df1'] != df_diff['areasprotegidaskm2_df2']))]

    df_update = df_diff[~df_diff['areasprotegidaskm2_df2'].isna()]
    novo   = df_diff[df_diff['areasprotegidaskm2_df2'].isna()]

    df_update = df_update[['codmun', 'areamunkm2_df1', 'areasprotegidaskm2_df1', 'percentual_df1','nome_sigla']]
    novo = novo[['codmun', 'areamunkm2_df1', 'areasprotegidaskm2_df1', 'percentual_df1','nome_sigla']]
    df_update.columns = ['codmun', 'areamunkm2', 'areasprotegidaskm2', 'percentual','nome_sigla']
    novo.columns = ['codmun', 'areamunkm2', 'areasprotegidaskm2', 'percentual','nome_sigla']

    df_update = df_update.dropna()

    return novo, df_update
    
def run_table_percentual_areas_protegidas():
    try:
        download_shape()   
        df = dataframe() 
        df_novo, df_update = get_df_novo_update(df)
        if df_novo.shape[0]:
            add_values(df_novo, table_name)
        if df_update.shape[0]:    
            update_table_percentual_areas_protegidas(df_update, table_name)
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")         