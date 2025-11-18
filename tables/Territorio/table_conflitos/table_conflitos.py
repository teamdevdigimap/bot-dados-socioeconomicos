from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
import os
import shutil
from glob import glob
import pandas as pd
from utils.utils import get_table_conflitos, get_municipio,add_values

table_name = 'table_conflitos'

local_download = "tables/Territorio/Conflitos"

def download_csv():
    
    download_dir = local_download
    
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        
    options = webdriver.ChromeOptions()
    
    #options.add_argument("--headless")  # Executar sem abrir a janela do navegador
    options.add_argument("--no-sandbox")  # Para evitar problemas no ambiente de contêineres
    options.add_argument("--disable-dev-shm-usage")  # Para evitar problemas de memória em containers
    
    download_dir = f'{os.getcwd()}/{download_dir}'

    # Alterar o diretório de download
    prefs = {
        "download.default_directory": download_dir,  # Definir o diretório de download
        "download.prompt_for_download": False,        # Desabilitar o prompt de download
        "directory_upgrade": True                     # Forçar o Chrome a usar o diretório de download especificado
    }
    options.add_experimental_option("prefs", prefs)
  

    # Garantir que o chromedriver seja baixado e executado corretamente
    driver_path = ChromeDriverManager().install()

    # Inicializar o WebDriver com o caminho correto do chromedriver
    driver = webdriver.Chrome(service=Service(driver_path), options=options)
    
    driver.get("https://mapadeconflitos.ensp.fiocruz.br")
    
    
    
    time.sleep(45)
    
    try:
        download_button = driver.find_element(By.CSS_SELECTOR, '.dt-button.buttons-excel.buttons-html5')
        download_button.click()
        time.sleep(30)
    except Exception as e:
        print(f"Erro : {e}")
        

def dataframe():
    excel = glob(f"{local_download}/*.xlsx")[0]
    df = pd.read_excel(excel, skiprows=1)
    df = df.drop(columns=['Link','UF'])
    # df[(df['Município'].str.contains(','))]
    df = df.set_index(df.columns.drop('Município').tolist())['Município'].str.split(',', expand=True).stack().reset_index(name='Município')
    df = df.drop(columns='level_5')
    # print(df.shape[0])
    df['Município'] = df['Município'].replace(r' \((.*?)\)', r' - \1', regex=True)
    df.columns = ['nome', 'populacoes','atividadesgeradorasdoconflito', 'danosasaude', 'impactossocioambientais', 'nome_sigla']
    mun = get_municipio()
    df = pd.merge(df,mun, on='nome_sigla')
    return df
        

def get_df_novo_update(df):
    df_banco = get_table_conflitos(table_name)
    df_banco = df_banco.drop(columns=['mes','ano'])
    
    merged = pd.merge(df, df_banco, how='left', on=['codmun','nome_sigla'], suffixes=['_novo','_banco'])
    
    df_novo = merged[merged['nome_banco'].isna()]
    df_novo = df_novo[['nome_novo', 'populacoes_novo', 'atividadesgeradorasdoconflito_novo', 'danosasaude_novo','impactossocioambientais_novo', 'nome_sigla', 'codmun']]
    df_novo.columns = ['nome', 'populacoes', 'atividadesgeradorasdoconflito', 'danosasaude','impactossocioambientais', 'nome_sigla', 'codmun']
    
    return df_novo
        
def run_table_conflitos():
    try:
        download_csv()
        df = dataframe()
        df_novo  = get_df_novo_update(df)
        if df_novo.shape[0]:
            add_values(df_novo,table_name)
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")         