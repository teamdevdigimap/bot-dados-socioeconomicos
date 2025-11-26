import os
import time
import shutil
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.edge.options import Options
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = "table_tx_suicidio"

df_municipios = None

def insert_manual_tx_suicidio_from_csv(file_path, sep=';'):
    """
    Lê um arquivo CSV contendo dados de Taxa de Suicídio e insere no banco.
    Estrutura esperada do CSV: codmun, ano, txasuicidio
    
    Args:
        file_path (str): Caminho completo para o arquivo .csv
        sep (str): Separador do CSV. 
                   Use ';' para CSVs padrão excel brasileiro.
                   Use '\t' se os dados estiverem separados por tabulação (como no exemplo copiado).
    """
    print(f"\n---> Processando arquivo manual: {file_path} ---")

    try:
        # 1. Leitura do arquivo
        df = pd.read_csv(file_path, sep=sep, dtype={'codmun': str})
        
        # Normalização dos nomes das colunas (remove espaços e coloca em minúsculo)
        df.columns = df.columns.str.strip().str.lower()
        
        colunas_esperadas = ['codmun', 'ano', 'txasuicidio']
        
        # Validação básica
        if not all(col in df.columns for col in colunas_esperadas):
             print(f"Colunas encontradas: {df.columns.tolist()}")
             raise Exception(f"O CSV deve conter as colunas: {colunas_esperadas}")

        # 2. Tratamento de dados
        df['txasuicidio'] = df['txasuicidio'].astype(str).str.replace(',', '.', regex=False)
        df['txasuicidio'] = pd.to_numeric(df['txasuicidio'], errors='coerce')
        
        df['ano'] = pd.to_numeric(df['ano'], errors='coerce').astype('Int64') # Int64 permite nulos se necessário

        # Remove linhas onde codmun ou ano sejam nulos (são chaves essenciais)
        df = df.dropna(subset=['codmun', 'ano'])

        # 3. Cruzamento com Tabela de Municípios (Utils) para pegar nome_sigla
        global df_municipios
        if df_municipios is None or df_municipios.empty:
            df_municipios = get_municipio()
            # Garante que codmun no df de municipios seja string para o merge
            if 'codmun' in df_municipios.columns:
                df_municipios['codmun'] = df_municipios['codmun'].astype(str)

        # Merge para pegar o nome_sigla
        df_final = df.merge(
            df_municipios[['codmun', 'nome_sigla']], 
            on='codmun', 
            how='left'
        )

        # 4. Preparação final
        df_insert = df_final[['codmun', 'ano', 'txasuicidio', 'nome_sigla']].copy()

        # 5. Inserção
        if not df_insert.empty:
            print(f"Inserindo {len(df_insert)} registros manuais na tabela {table_name}...")
            # print(df_insert.head())
            add_values(df_insert, table_name)
        else:
            print("Nenhum dado válido para inserir.")

    except FileNotFoundError:
        print(f"Erro: O arquivo '{file_path}' não foi encontrado.")
    except Exception as e:
        print(f"Erro crítico ao processar o arquivo manual: {e}")

def download_data(download_path):
    """ Função para baixar os dados de Taxa de Suicídio do site do Atlas da Violência """
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

    
    driver = webdriver.Chrome( options=options)

    url = "https://www.ipea.gov.br/atlasviolencia/filtros-series"
    driver.get(url)
    wait = WebDriverWait(driver, 20)

    try:
        # Clicar na seção "Suicídio"
        suicidio_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//h2[contains(text(),'Suicídio')]"))
        )
        suicidio_button.click()
        time.sleep(3)

        # Esperar o campo de pesquisa
        campo_pesquisa = wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Pesquisa pelo nome']"))
        )
        campo_pesquisa.send_keys("Taxa de Suicídio")
        time.sleep(5)

        # Selecionar a linha exata com "Taxa de Suicídio"
        linha_taxa = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//td[normalize-space(text())='Taxa de Suicídio']/..")
            )
        )
        icone_csv = linha_taxa.find_element(By.XPATH, ".//i[@class='fa fa-file-excel-o']")
        icone_csv.click()

        time.sleep(10)
        
        # Esperar e clicar no botão de download para Município
        download_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//div[text()='Município']/.."))
        )
        time.sleep(3)
        download_button.click()

        time.sleep(20)  # Tempo para garantir o download do arquivo
    except Exception as e:
        driver.quit()
        raise Exception(f"Erro ao baixar o arquivo: {e}")

    driver.quit()

def process_dataframe(download_path):
    """ Função para processar o CSV baixado """
    csv_path = os.path.join(download_path, "taxa-de-suicidio.csv")

    if not os.path.exists(csv_path):
        raise Exception(f"Arquivo CSV não encontrado no caminho esperado: {csv_path}")

    df = pd.read_csv(csv_path, sep=";")
    
    # Renomear colunas para seguir o modelo do banco
    df.rename(columns={
        'cod': 'codmun',
        'valor': 'txasuicidio',
        'nome': 'nome_sigla',
        'período': 'ano'
    }, inplace=True)

    df = df[['codmun', 'ano', 'txasuicidio', 'nome_sigla']]
    
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
            # print(df)
            add_values(df, table_name)

        shutil.rmtree(script_dir)  # Remover diretório após processamento
    except Exception as e:
        raise Exception(f"Erro no processamento dos dados: {e}")

def run_table_taxa_suicidio():
    """ Função para executar o fluxo de atualização da tabela """
    try:
        dataframe() #comente esta linha se for inserir dados manualmente
        
        # para atualização via arquivo manual
        # Exemplo: caminho_csv = "C:/Users/matheus.souza/Downloads/suicidios.csv"
        caminho_csv = "C:/Users/matheus.souza/Downloads/suicidios.csv"
        
        if caminho_csv is not None:
            try:
                if os.path.exists(caminho_csv):
                    # Ajuste o sep conforme seu arquivo (ex: ',' para CSV padrão, '\t' para tabulação)
                    insert_manual_tx_suicidio_from_csv(caminho_csv, sep=';')
                else:
                    print(f"Arquivo CSV manual não encontrado em: {caminho_csv}.")
            except Exception as e:
                print(f"Erro ao inserir dados manuais do CSV: {e}")
        elif caminho_csv is None:
            print("Nenhum arquivo CSV manual fornecido para inserção.")
            
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")
