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
import logging

logging.basicConfig(level=logging.WARNING)
table_name = 'table_percentual_areas_protegidas'

base_dir = os.path.join(os.path.expanduser("~"), "Downloads")
local_download = os.path.join(base_dir, "tables", "Territorio", "AreasProtegidas")

def download_shape():

    download_dir = os.path.abspath(local_download)

    # Limpeza segura para evitar "Acesso Negado"
    # Ao invés de deletar a pasta inteira, deletamos apenas o conteúdo
    if not os.path.exists(download_dir):
        os.makedirs(download_dir, exist_ok=True)
    else:
        print(f"Limpando diretório: {download_dir}")
        files = glob(os.path.join(download_dir, "*"))
        for f in files:
            try:
                if os.path.isfile(f):
                    os.remove(f)
                elif os.path.isdir(f):
                    shutil.rmtree(f)
            except Exception as e:
                print(f"Aviso: Não foi possível deletar {f}: {e}")

    # Configurar o Selenium
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new") 
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")

    # Configurações de preferência
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.automatic_downloads": 1
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)

    try:
        logging.info("Acessando site...")
        driver.get("https://cnuc.mma.gov.br/map")
        time.sleep(8) 

        # 1. Clicar no botão inicial
        logging.info("Tentando clicar no primeiro botão de download...")
        download_button = driver.find_element(By.XPATH, "//button[@type='button' and contains(text(),'Download do geo da UC:')]")
        driver.execute_script("arguments[0].click();", download_button) 
        
        time.sleep(3)
        
        # 2. Selecionar SHP
        logging.info("Selecionando formato SHP...")
        shp_radio_button = driver.find_element(By.XPATH, "//input[@class='jss4' and @value='shp']")
        driver.execute_script("arguments[0].click();", shp_radio_button)

        time.sleep(3)
        
        # 3. Download Final
        logging.info("Clicando no botão final de download...")
        download_button_final = driver.find_element(By.XPATH, "//button[@type='button' and contains(text(),'Download') and contains(@class, 'btn-success')]")
        driver.execute_script("arguments[0].click();", download_button_final)

        # Monitoramento do Download
        timeout = 180 
        start_time = time.time()
        download_finalizado = False
        
        logging.info("Aguardando arquivo .zip...")
        while time.time() - start_time < timeout:
            arquivos = os.listdir(download_dir)
            # Verifica se existe .zip e se NÃO existe .crdownload
            if any(f.endswith('.zip') for f in arquivos) and not any(f.endswith('.crdownload') for f in arquivos):
                download_finalizado = True
                break
            time.sleep(1)

        if not download_finalizado:
            print(f"Conteúdo do diretório após timeout: {os.listdir(download_dir)}")
            raise Exception("Tempo limite de download excedido.")

    except Exception as e:
        driver.quit()
        raise Exception(f"Falha no processo do Selenium: {e}")

    driver.quit()
    
    # Processamento do arquivo
    arquivos_no_dir = os.listdir(download_dir)
    lista_zips = [i for i in arquivos_no_dir if ".zip" in i]
    
    if not lista_zips:
        raise Exception(f"Erro: Download concluído, mas nenhum .zip encontrado em {download_dir}.")
    
    nome_arquivo_zip = lista_zips[0]
    caminho_completo_zip = os.path.join(download_dir, nome_arquivo_zip)
    
    logging.info(f"Descompactando {nome_arquivo_zip}...")
    with zipfile.ZipFile(caminho_completo_zip, 'r') as zip_ref:
        zip_ref.extractall(download_dir)
    
    print(f"Arquivos descompactados com sucesso em: {download_dir}")
    
def get_shp_uc():
    pattern = os.path.join(local_download, "*.shp")
    arquivos = glob(pattern)
    if arquivos:
        arquivo = arquivos[0]
        print(f"Lendo Shapefile: {arquivo}")
        df = gpd.read_file(arquivo)
        return df
    return None

def dataframe():
    mun = get_municipio()
    shp = get_shp_uc()
    if shp is None:
        raise Exception("Nenhum arquivo .shp encontrado após download e extração.")
    
    shp['geometry'] = shp.buffer(0)
    
    # Caminho do shape de municípios (Ajuste se necessário para caminho absoluto se der erro)
    path_municipios = "tables/Territorio/Shp_Municipios/BR_Municipios_2022.shp"
    if not os.path.exists(path_municipios):
         raise Exception(f"Arquivo de municípios não encontrado: {path_municipios}")

    municipios = gpd.read_file(path_municipios) 
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
    
    # Garantir tipo string para merge correto
    df['codmun'] = df['codmun'].astype(str)
    if 'codmun' in mun.columns:
        mun['codmun'] = mun['codmun'].astype(str)

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
            print(f"Adicionando {len(df_novo)} novos registros na tabela {table_name}.")
            add_values(df_novo, table_name)
        if df_update.shape[0]:    
            print(f"Atualizando {len(df_update)} registros na tabela {table_name}.")
            update_table_percentual_areas_protegidas(df_update, table_name)
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")