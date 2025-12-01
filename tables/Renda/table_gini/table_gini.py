import pandas as pd
import basedosdados as bd
from utils.utils import get_ultimo_ano, add_values, update_chaves_municipios
import os
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
load_dotenv()

# --- CONFIGURAÇÃO E DEBUG INICIAL ---
TABLE_NAME = 'table_renda_per_capta'

# ATENÇÃO: Verifique se a variável de ambiente que guarda o ID do projeto é realmente 'USER'.
# Geralmente é algo como 'GOOGLE_CLOUD_PROJECT' ou um nome personalizado definido no .env.
BILLING_PROJECT_ID = os.environ.get('USER') 

print(f"\n[DEBUG] --- Configurações Iniciais ---")
print(f"[DEBUG] Tabela Alvo: {TABLE_NAME}")
print(f"[DEBUG] Billing Project ID detectado: '{BILLING_PROJECT_ID}'")

if not BILLING_PROJECT_ID:
    print("[ERRO CRÍTICO] O Billing Project ID está vazio ou None. Verifique seu arquivo .env ou variáveis de sistema.")
else:
    print(f"[DEBUG] Credencial parece preenchida. Prosseguindo...")
# ------------------------------------

def download_and_transform(ano_inicio):
    """
    Busca o PIB per capita municipal no Base dos Dados para preencher a lacuna anual.
    Retorna um DataFrame formatado para inserção.
    """
    
    query = f"""
    SELECT 
        SUBSTR(id_municipio, 1, 6) as codmun,
        ano,
        pib_per_capita as renda
    FROM `basedosdados.br_ibge_pib.municipio`
    WHERE ano > {ano_inicio}
    """

    print(f"\n[DEBUG] --- Iniciando Download ---")
    print(f"[DEBUG] Query montada para execução:\n{query.strip()}")
    
    try:
        print(f"[DEBUG] Enviando requisição ao BigQuery usando projeto: {BILLING_PROJECT_ID}...")
        
        # O parâmetro billing_project_id é obrigatório para consultas no BigQuery
        df = bd.read_sql(query, billing_project_id=BILLING_PROJECT_ID)
        
        print(f"[DEBUG] Retorno do BigQuery obtido. Linhas: {df.shape[0]}, Colunas: {df.shape[1]}")
        
    except Exception as e:
        print(f"[ERRO] Falha ao consultar Base dos Dados: {e}")
        # Dica extra de debug baseada no erro comum de projeto
        if "Access Denied" in str(e) or "project" in str(e).lower():
            print("[DICA] Verifique se o BILLING_PROJECT_ID é válido e se tem permissão de uso do BigQuery.")
        return pd.DataFrame()

    if df.empty:
        print(f"[DEBUG] A consulta rodou com sucesso, mas retornou ZERO linhas. (Nenhum dado novo > {ano_inicio})")
        return df

    print(f"[DEBUG] Aplicando transformações de tipos nos dados...")
    try:
        # Garantia de tipagem para evitar erros no banco PostgreSQL
        df['codmun'] = df['codmun'].astype(int)
        df['ano'] = df['ano'].astype(int)
        df['renda'] = df['renda'].astype(float)
        print(f"[DEBUG] Transformação concluída. Amostra dos dados:\n{df.head(3)}")
    except Exception as e:
        print(f"[ERRO] Erro ao transformar tipos dos dados: {e}")
        return pd.DataFrame()

    return df

def run_table_renda_per_capta():
    print(f"\n[DEBUG] --- Iniciando Rotina Principal ---")
    
    # 1. Verifica o último ano existente no banco para buscar apenas o delta
    ultimo_ano = 0
    try:
        print(f"[DEBUG] Consultando último ano na tabela local '{TABLE_NAME}'...")
        ultimo_ano = get_ultimo_ano(TABLE_NAME)
        print(f"[DEBUG] Último ano encontrado no banco: {ultimo_ano}")
    except Exception as e:
        print(f"[ALERTA] Não foi possível obter o último ano (Tabela vazia ou erro de conexão): {e}")
        print(f"[DEBUG] Assumindo ano inicial = 0 (baixar histórico completo).")
        ultimo_ano = 0

    # 2. Busca dados novos (Delta)
    df_new = download_and_transform(ultimo_ano)
    
    # 3. Insere e Atualiza
    if not df_new.empty:
        print(f"\n[DEBUG] --- Iniciando Inserção no Banco ---")
        print(f"[DEBUG] Tentando inserir {len(df_new)} novos registros...")
        
        try:
            add_values(df_new, TABLE_NAME)
            print(f"[DEBUG] Função add_values executada.")
        except Exception as e:
            print(f"[ERRO] Falha na função add_values: {e}")
            return # Interrompe se falhar aqui
        
        # 4. Atualiza colunas auxiliares
        print(f"[DEBUG] Executando update_chaves_municipios para a tabela {TABLE_NAME}...")
        try:
            update_chaves_municipios(TABLE_NAME)
            print(f"[DEBUG] Chaves atualizadas com sucesso.")
        except Exception as e:
             print(f"[ERRO] Falha ao atualizar chaves: {e}")

        print("\n[SUCESSO] Atualização concluída.")
    else:
        print(f"\n[AVISO] Processo finalizado sem alterações. Motivo: DataFrame vazio (sem dados novos após {ultimo_ano}).")

