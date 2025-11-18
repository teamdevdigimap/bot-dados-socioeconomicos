import os
import time
import shutil
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = "table_tx_obito_acidentes_transito"

def download_data(download_path):
    """ Função para baixar os dados do site do Atlas da Violência """
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")

    # Definir diretório de download
    options.add_experimental_option("prefs", {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    })

    # service = Service(EdgeChromiumDriverManager().install())
    # driver = webdriver.Edge(service=service, options=options)

    
    driver = webdriver.Chrome(options=options)
    
    url = "https://www.ipea.gov.br/atlasviolencia/filtros-series"
    driver.get(url)
    wait = WebDriverWait(driver, 20)

    try:
        # Clicar no botão "Violência no Trânsito"
        acidentes_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//h2[contains(text(),'Violência no Trânsito')]")) 
        )
        acidentes_button.click()
        time.sleep(45)

        # Esperar o campo de pesquisa
        campo_pesquisa = wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Pesquisa pelo nome']"))
        )
        campo_pesquisa.send_keys("Taxa de Óbitos em Acidentes de Transporte")
        time.sleep(45)

        # Selecionar a linha correspondente
        # Selecionar a linha exata com "Taxa de Óbitos em Acidentes de Transporte"
        linha_taxa = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//td[normalize-space(text())='Taxa de óbitos em acidentes de transporte']/..")
            )
        )
        icone_csv = linha_taxa.find_element(By.XPATH, ".//i[@class='fa fa-file-excel-o']")
        icone_csv.click()

        time.sleep(45)
        
        # Esperar e clicar no botão de download para Município
        download_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//div[text()='Município']/.."))
        )
        time.sleep(45)
        download_button.click()

        time.sleep(45)  # Tempo para garantir o download do arquivo
    except Exception as e:
        driver.quit()
        raise Exception(f"Erro ao baixar o arquivo: {e}")

    driver.quit()

def process_dataframe(download_path):
    """ Função para processar o CSV baixado """
    csv_path = os.path.join(download_path, "taxa-de-obitos-em-acidentes-de-transporte.csv")

    if not os.path.exists(csv_path):
        raise Exception(f"Arquivo CSV não encontrado no caminho esperado: {csv_path}")

    df = pd.read_csv(csv_path, sep=";")
    
    # Renomear colunas conforme esperado
    df.rename(columns={
        'cod': 'codmun',
        'valor': 'txaobitosaacidentesatransito',
        'nome': 'nome_sigla',
        'período': 'ano'
    }, inplace=True)

    df = df[['codmun', 'ano', 'txaobitosaacidentesatransito', 'nome_sigla']]
    
    # Obter último ano já registrado no banco
    ultimo_ano = get_ultimo_ano(table_name)
    
    # Filtrar apenas anos a partir de 2010 e que ainda não foram adicionados ao banco
    #df = df[df['ano'] >= 2010]
    df = df[df['ano'] > ultimo_ano]

    # Ajustar os tipos de dados
    df['codmun'] = df['codmun'].astype(str)
    
    # Associar municípios
    mun = get_municipio()
    mun['codmun'] = mun['codmun'].astype(str)

    # Fazer o merge
    df = pd.merge(df, mun, on='codmun', how='left')

    # Manter apenas a coluna correta e renomeá-la
    df = df.drop(columns=['nome_sigla_x'])
    df.rename(columns={'nome_sigla_y': 'nome_sigla'}, inplace=True)

    return df

def dataframe():
    """ Função principal para buscar e processar os dados """
    script_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tables', 'AtlasViolencia', table_name)
    os.makedirs(script_dir, exist_ok=True)

    try:
        # Baixar os dados
        download_data(script_dir)

        # Processar o dataframe
        df = process_dataframe(script_dir)

        # Inserir os novos dados no banco, se necessário
        if df.shape[0]:
            #print(df)
            add_values(df, table_name)

        shutil.rmtree(script_dir)  # Remover diretório após processamento
    except Exception as e:
        raise Exception(f"Erro no processamento dos dados: {e}")

def run_table_taxa_obitos_acidentes():
    """ Função para executar o fluxo de atualização da tabela """
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")
