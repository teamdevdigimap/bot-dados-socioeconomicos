from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
import os
import shutil
from glob import glob
import pandas as pd
from utils.utils import get_table_conflitos, get_municipio, add_values

table_name = 'table_conflitos'

# Define o diretório na pasta Documentos do usuário atual
# Isso resolve o problema de caminhos relativos e permissões em pastas de sistema
base_dir = os.path.join(os.path.expanduser("~"), "Documents")
local_download = os.path.join(base_dir, "tables", "Territorio", "Conflitos")

def download_csv():
    # Garante que o diretório existe
    if not os.path.exists(local_download):
        os.makedirs(local_download)
    else:
        # Limpeza segura (não deleta a pasta, apenas os arquivos)
        print(f"Limpando arquivos antigos em: {local_download}")
        files = glob(os.path.join(local_download, "*"))
        for f in files:
            try:
                os.remove(f) # Deleta arquivo por arquivo
            except Exception as e:
                print(f"Não foi possível deletar {f}: {e}")

    options = webdriver.ChromeOptions()
    
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    # Caminho absoluto para o Selenium 
    # O Chrome PRECISA do caminho absoluto completo, sem atalhos relativos
    abs_download_dir = os.path.abspath(local_download)
    print(f"Diretório de download configurado para: {abs_download_dir}")

    prefs = {
        "download.default_directory": abs_download_dir,
        "download.prompt_for_download": False,
        "directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
  
    driver_path = ChromeDriverManager().install()
    driver = webdriver.Chrome(service=Service(driver_path), options=options)
    
    try:
        print("Acessando site...")
        driver.get("https://mapadeconflitos.ensp.fiocruz.br")
        
        # Tempo aumentado para garantir carregamento (site pesado)
        time.sleep(45)
        
        print("Tentando clicar no botão de download...")
        download_button = driver.find_element(By.CSS_SELECTOR, '.dt-button.buttons-excel.buttons-html5')
        download_button.click()
        
        # Loop de espera ativa pelo arquivo (melhor que time.sleep fixo)
        timeout = 60
        start_time = time.time()
        while time.time() - start_time < timeout:
            xlsx_files = glob(os.path.join(local_download, "*.xlsx"))
            # Verifica se existe arquivo e se não é um temporário (.crdownload)
            if xlsx_files and not glob(os.path.join(local_download, "*.crdownload")):
                print("Download concluído!")
                break
            time.sleep(1)
            
    except Exception as e:
        print(f"Erro no Selenium: {e}")
    finally:
        driver.quit() # Garante que o navegador feche e solte os arquivos

def dataframe():
    # Busca o arquivo baixado
    arquivos = glob(f"{local_download}/*.xlsx")
    
    if not arquivos:
        raise Exception(f"Nenhum arquivo Excel encontrado em {local_download}")
        
    excel = arquivos[0]
    print(f"Processando arquivo: {excel}")
    
    df = pd.read_excel(excel, skiprows=1)
    
    # Tratamento caso a coluna Link ou UF não existam (previne erros se o layout mudar)
    cols_drop = [c for c in ['Link','UF'] if c in df.columns]
    df = df.drop(columns=cols_drop)
    
    # Lógica original de tratamento
    df = df.set_index(df.columns.drop('Município').tolist())['Município'].str.split(',', expand=True).stack().reset_index(name='Município')
    
    if 'level_5' in df.columns:
        df = df.drop(columns='level_5')
        
    df['Município'] = df['Município'].replace(r' \((.*?)\)', r' - \1', regex=True)
    df.columns = ['nome', 'populacoes','atividadesgeradorasdoconflito', 'danosasaude', 'impactossocioambientais', 'nome_sigla']
    
    mun = get_municipio()
    df = pd.merge(df, mun, on='nome_sigla')
    return df
        

def get_df_novo_update(df):
    df_banco = get_table_conflitos(table_name)
    print("os dados a serem inseridos são:\n", df.head(10))
    
    # Verifica se df_banco retornou dados antes de tentar limpar colunas
    if df_banco is not None and not df_banco.empty:
        # Remove colunas que não existem no banco ou não são necessárias para comparação
        # cols_drop = [c for c in ['mes','ano'] if c in df_banco.columns]
        # df_banco = df_banco.drop(columns=cols_drop)
        
        merged = pd.merge(df, df_banco, how='left', on=['codmun','nome_sigla'], suffixes=['_novo','_banco'])
        
        # Filtra onde não houve match no banco (novos registros)
        df_novo = merged[merged['nome_banco'].isna()]
    else:
        # Se o banco está vazio, tudo é novo
        print("Tabela do banco vazia ou inexistente. Inserindo tudo.")
        df_novo = df.copy()
        # Adiciona sufixos virtuais para manter compatibilidade com o código abaixo
        df_novo = df_novo.rename(columns={
            'nome': 'nome_novo', 
            'populacoes': 'populacoes_novo',
            'atividadesgeradorasdoconflito': 'atividadesgeradorasdoconflito_novo',
            'danosasaude': 'danosasaude_novo',
            'impactossocioambientais': 'impactossocioambientais_novo'
        })

    # Seleciona e renomeia colunas finais
    colunas_finais = {
        'nome_novo': 'nome', 
        'populacoes_novo': 'populacoes', 
        'atividadesgeradorasdoconflito_novo': 'atividadesgeradorasdoconflito', 
        'danosasaude_novo': 'danosasaude',
        'impactossocioambientais_novo': 'impactossocioambientais'
    }
    
    # Garante que as colunas existem antes de filtrar
    cols_existentes = [c for c in df_novo.columns if c in colunas_finais.keys() or c in ['nome_sigla', 'codmun']]
    df_novo = df_novo[cols_existentes]
    
    df_novo = df_novo.rename(columns=colunas_finais)
    
    return df_novo
        
def run_table_conflitos():
    try:
        download_csv()
        df = dataframe()
        df_novo  = get_df_novo_update(df)
        
        if df_novo.shape[0]:
            print(f"Inserindo {len(df_novo)} novos registros...")
            add_values(df_novo, table_name)
        else:
            print("Nenhum registro novo encontrado.")
            
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")