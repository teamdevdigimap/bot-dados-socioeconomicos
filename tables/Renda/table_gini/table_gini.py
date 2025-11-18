import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import shutil
from utils.utils import get_municipio_codmun6, get_ultimo_ano, add_values
from datetime import datetime

table_name = 'table_gini'
def dataframe():
    gini = pd.read_csv("GINI/ginibr.csv",encoding = "ISO-8859-1", on_bad_lines='skip', sep=';', skiprows=2, skipfooter=2)
    gini['Município'] = gini['Município'].str.slice(stop=7)
    gini['codmun6'] = gini['Município'].astype(int)
    df = gini.melt(id_vars=['codmun6'], value_vars=['1991', '2000', '2010'], 
                        var_name='ano', value_name='gini')
    df['ano'] = df['ano'].astype(int)
    df['gini'] = df['gini'].str.replace(",",".")
    df = df[df['gini']!= '...']
    df['gini'] = df['gini'].astype(float)
    df

    mun = get_municipio_codmun6()
    mun['codmun6'] = mun['codmun6'].astype(int) 

    df = pd.merge(df, mun, how='left', on='codmun6')
    # df = df.drop(columns='codmun6')
    df = df[df['ano'] >= 2010]
    df.columns = ['codmun', 'ano','gini','codmun7','nome_sigla']
    return df

def download_csv():
    download_dir = "GINI"

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


    
    driver = webdriver.Chrome( options=options)

    # Acessar a página
    driver.get("http://tabnet.datasus.gov.br/cgi/ibge/censo/cnv/ginibr.def")

    # Esperar a página carregar
    time.sleep(15)

    # Encontrar o botão de download e clicar
    try:
        # Clicar no botão de download inicial
        wait = WebDriverWait(driver, 10)  # Wait for up to 10 seconds
        link = wait.until(EC.element_to_be_clickable((By.XPATH, '//td[@class="botao_opcao"]//a[text()="Copia como .CSV"]')))
        link.click()
        time.sleep(15)

    except Exception as e:
        #print(f"Erro ao encontrar o botão de download ou selecionar o formato: {e}")
        raise Exception(f"Erro ao clicar no link de download: {e}")
    # # Fechar o navegador
    driver.quit()
    

def run_table_gini():
    download_csv() 
    df = dataframe()
    ultimo_ano = get_ultimo_ano(table_name) 
    df = df[df['ano'] > ultimo_ano]
    if df.shape[0]:
        add_values(df,table_name)

