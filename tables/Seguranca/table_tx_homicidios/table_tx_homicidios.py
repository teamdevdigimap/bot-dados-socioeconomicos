import os
import time
import shutil
import pandas as pd
from selenium import webdriver
# from selenium.webdriver.edge.service import Service
# from selenium.webdriver.edge.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import traceback
from utils.utils import add_values,get_ultimo_ano, get_municipio

table_name = "table_tx_homicidios"

def download_data(download_path):
    """ Função para baixar os dados do site do Atlas da Violência """
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Rodar sem abrir o navegador
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    # options.add_argument("--window-size=1920x1080")
    # options.add_argument("--disable-blink-features=AutomationControlled")

    # Definir diretório de download
    options.add_experimental_option("prefs", {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    })


    # service = Service(ChromeDriverManager().install())
    # driver = webdriver.Chrome(service=service, options=options)
    
    # Inicializar o WebDriver com o caminho correto do chromedriver
    driver = webdriver.Chrome(options=options)

    url = "https://www.ipea.gov.br/atlasviolencia/filtros-series"
    driver.get(url)
    wait = WebDriverWait(driver, 20)

    try:
        # Clicar no botão "Homicídios"
        homicidios_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//h2[text()='Homicídios']"))
        )
        homicidios_button.click()

        # Esperar o campo de pesquisa
        campo_pesquisa = wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Pesquisa pelo nome']"))
        )
        campo_pesquisa.send_keys("Taxa Homicídios")
        time.sleep(75)

        # Selecionar a linha correspondente
        linha_homicidios = driver.find_element(By.XPATH, "//td[text()='Taxa Homicídios']/..")
        icone_csv = linha_homicidios.find_element(By.XPATH, ".//i[@class='fa fa-file-excel-o']")
        icone_csv.click()

        # Esperar e clicar no botão de download
        download_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//div[text()='Município']/.."))
        )
        time.sleep(60)
        download_button.click()

        time.sleep(15)  # Tempo para download
    except Exception as e:
        driver.quit()
        raise Exception(f"Erro ao baixar o arquivo: {e}")

    driver.quit()

def process_dataframe(download_path):
    """ Função para processar o CSV baixado """
    csv_path = os.path.join(download_path, "taxa-homicidios.csv")

    if not os.path.exists(csv_path):
        raise Exception("Arquivo CSV não encontrado após download.")

    df = pd.read_csv(csv_path, sep=";")
    df.columns = ['codmun', 'nome', 'ano', 'txahomicidios']
    df = df.drop(columns=['nome'])

    # Obter último ano já registrado
    ultimo_ano = get_ultimo_ano(table_name)
    
    # Filtrar apenas anos mais recentes
    df = df[df['ano'] > ultimo_ano]
    
    # Ajustar os tipos de dados
    df['codmun'] = df['codmun'].astype(str)
    
    # Associar municípios
    mun = get_municipio()
    mun['codmun'] = mun['codmun'].astype(str)
    df = pd.merge(df, mun, on='codmun', how='left')

    return df

def dataframe():
    """ Função principal para buscar e processar os dados """
    #script_dir = os.path.join(os.getcwd(), 'tables', 'AtlasViolencia', table_name)
    script_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tables', 'AtlasViolencia', table_name)


    os.makedirs(script_dir, exist_ok=True)

    # Baixar os dados
    download_data(script_dir)

    # Processar o dataframe
    df = process_dataframe(script_dir)

    # Inserir os novos dados no banco, se necessário
    if df.shape[0]:
        #print(df)
        add_values(df, table_name)

    shutil.rmtree(script_dir)  # Remover diretório após processamento

def run_table_taxa_homicidios():
    """ Função para executar o fluxo de atualização da tabela """
    try:
        dataframe()
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")
