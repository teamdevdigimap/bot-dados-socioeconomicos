import basedosdados as bd
from datetime import datetime
import pandas as pd
import os
import shutil
import zipfile
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import traceback
from utils.utils import add_values, get_ultimo_ano, get_municipio


# Definir nome da tabela
table_name = 'table_taxas_de_reprovacao'

# Obter último ano registrado e definir intervalo de anos
ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1


def download_and_extract_data(download_path):

    options = webdriver.ChromeOptions()#Options()
    
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")


    options.add_experimental_option("prefs", {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(options=options)
    
    url = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/indicadores-educacionais/taxas-de-rendimento-escolar"
    driver.get(url)
    wait = WebDriverWait(driver, 10)

    try:
        cookies_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Rejeitar cookies')]"))
        )
        cookies_button.click()
    except:
        pass
    
    try:
        zip_download_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="parent-fieldname-text"]/ul/li[2]/a')))
        zip_download_button.click()
    except:
        driver.quit()
        raise Exception("Botão de download não encontrado.")
    
    time.sleep(25)  # Tempo para download
    driver.quit()

    files = [f for f in os.listdir(download_path) if f.endswith(".zip")]
    if not files:
        raise Exception("Arquivo ZIP não encontrado.")
    
    zip_path = os.path.join(download_path, files[0])
    extract_folder = os.path.join(download_path, "dados_extraidos")
    os.makedirs(extract_folder, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)
    
    os.remove(zip_path)  # Exclui o arquivo ZIP após a extração
    return extract_folder

def process_dataframe(extract_folder):
    """ Função para processar os dados baixados """
    subfolders = [f for f in os.listdir(extract_folder) if os.path.isdir(os.path.join(extract_folder, f))]
    if not subfolders:
        raise Exception("Nenhuma subpasta encontrada dentro de 'dados_extraidos'.")
    
    subfolder_path = os.path.join(extract_folder, subfolders[0])
    xlsx_files = [f for f in os.listdir(subfolder_path) if f.endswith(".xlsx")]
    if not xlsx_files:
        raise Exception("Nenhum arquivo .xlsx encontrado na subpasta extraída.")
    
    xlsx_path = os.path.join(subfolder_path, xlsx_files[0])
    df = pd.read_excel(xlsx_path, engine="openpyxl", skiprows=8, skipfooter=2)
    
    df = df[['CO_MUNICIPIO','NO_CATEGORIA','NO_DEPENDENCIA', 'NU_ANO_CENSO',
             '2_CAT_FUN','2_CAT_FUN_AI', '2_CAT_FUN_AF', '2_CAT_FUN_01',
             '2_CAT_FUN_02', '2_CAT_FUN_03', '2_CAT_FUN_04', '2_CAT_FUN_05',
             '2_CAT_FUN_06', '2_CAT_FUN_07', '2_CAT_FUN_08', '2_CAT_FUN_09',
             '2_CAT_MED', '2_CAT_MED_01', '2_CAT_MED_02', '2_CAT_MED_03',
             '2_CAT_MED_04', '2_CAT_MED_NS']]
    
    df.columns = ['codmun', 'tipolocalizacao', 'rede', 'ano',
                  'taxareprovacaoensinofundamental', 'taxareprovacaoensinofundamentalanosiniciais',
                  'taxareprovacaoensinofundamentalanosfinais', 'taxareprovacaoensinofundamental1ano',
                  'taxareprovacaoensinofundamental2ano', 'taxareprovacaoensinofundamental3ano',
                  'taxareprovacaoensinofundamental4ano', 'taxareprovacaoensinofundamental5ano',
                  'taxareprovacaoensinofundamental6ano', 'taxareprovacaoensinofundamental7ano',
                  'taxareprovacaoensinofundamental8ano', 'taxareprovacaoensinofundamental9ano',
                  'taxareprovacaoensinomedio', 'taxareprovacaoensinomedio1ano',
                  'taxareprovacaoensinomedio2ano', 'taxareprovacaoensinomedio3ano',
                  'taxareprovacaoensinomedio4ano', 'taxareprovacaoensinomedionaoseriado']
    
    mun = get_municipio()
    df['codmun'] = df['codmun'].astype(str)
    mun['codmun'] = mun['codmun'].astype(str)

    df = pd.merge(df, mun, on='codmun', how='left')
    
    #Filtra o dataframe pela data
    df = df[df['ano'] >= ultimo_ano]
    
    return df

def dataframe():
    """ Função principal para buscar e processar os dados """
    script_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tables', 'Educacao', 'table_taxas_de_reprovacao')
    extract_folder = download_and_extract_data(script_dir)
    df = process_dataframe(extract_folder)
    if df.shape[0]:
        #print(df)
        add_values(df, table_name)
    
    shutil.rmtree(extract_folder)  # Remove a pasta extraída após processamento

def run_table_taxas_de_reprovacao():
    """ Função para executar o fluxo de atualização da tabela """
    try:
        dataframe()
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")
