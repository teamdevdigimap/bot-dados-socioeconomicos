import requests
import pandas as pd
from datetime import datetime
from io import StringIO
from functools import reduce
from utils.utils import add_values, get_ultimo_mes_ano, update_chaves_municipios

table_name = 'table_cadunico'

def buscar_dataframe(url, nome_etapa):
    """
    Função auxiliar para baixar e preparar cada pedaço dos dados.
    """
    # print(f"[{nome_etapa}] Iniciando requisição...")
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        # print(f"[{nome_etapa}] Erro na conexão: {e}")
        return None

    csv_data = StringIO(response.text)
    
    # Lê o CSV
    df = pd.read_csv(csv_data, sep=',', dtype=str)
    
    if df.empty:
        # print(f"[{nome_etapa}] Retorno vazio.")
        return None

    # Padronização inicial das chaves de cruzamento
    df = df.rename(columns={'codigo_ibge': 'codmun', 'anomes_s': 'data'})
    
    # Conversão de datas para extrair ano e mes
    df['data'] = pd.to_datetime(df['data'], format='%Y%m', errors='coerce')
    df['ano'] = df['data'].dt.year
    df['mes'] = df['data'].dt.month
    
    # Limpeza de linhas sem data e conversão para int das chaves
    df = df.dropna(subset=['ano', 'mes'])
    df['ano'] = df['ano'].astype(int)
    df['mes'] = df['mes'].astype(int)
    
    # Remove a coluna data original
    df = df.drop(columns=['data'])
    
    # print(f"[{nome_etapa}] Sucesso! Registros: {len(df)}")
    return df

def consultar_cadunico_ano(ano, mes_ref, ano_ref):
    """
    Consulta dados do CadÚnico para um ano específico,
    filtrando apenas dados posteriores à data de referência.
    """
    base_url = "https://aplicacoes.mds.gov.br/sagi/servicos/misocial"
    common_params = f"q=*:*&fq=anomes_s:{ano}*&rows=100000&wt=csv&sort=anomes_s%20desc,codigo_ibge%20asc"
    
    # 1. Família (Total)
    url_fam_total = f"{base_url}?fl=codigo_ibge,anomes_s,cadunicototfam:cadun_qtd_familias_cadastradas_i&{common_params}"
    
    # 2. Família (Detalhes: Extrema Pobreza, Pobreza, Baixa Renda, Acima 1/2 SM)
    url_fam_det = f"{base_url}?fl=codigo_ibge,anomes_s,cadunicototfamextpob:cadun_qtde_fam_sit_extrema_pobreza_s,cadunicototfampob:cadun_qtde_fam_sit_pobreza_s,cadunicototfamrpcatemeiosm:cadun_qtd_familias_cadastradas_baixa_renda_i&{common_params}"
    
    # 3. Pessoa (Total)
    url_pes_total = f"{base_url}?fl=codigo_ibge,anomes_s,cadunicototpes:cadun_qtd_pessoas_cadastradas_i&{common_params}"
    
    # 4. Pessoa (Detalhes: Pobreza, Baixa Renda, Extrema Pobreza)
    # Nota: O campo de extrema pobreza para pessoas não existe na API original, removido
    url_pes_det = f"{base_url}?fl=codigo_ibge,anomes_s,cadunicototpespob:cadun_qtd_pessoas_cadastradas_pobreza_pbf_i,cadunicototpesrpcatemeiosm:cadun_qtd_pessoas_cadastradas_baixa_renda_i&{common_params}"

    urls = [
        (url_fam_total, "Famílias Total"),
        (url_fam_det, "Famílias Detalhes"),
        (url_pes_total, "Pessoas Total"),
        (url_pes_det, "Pessoas Detalhes")
    ]

    dfs_para_juntar = []

    # Executa as 4 requisições
    for url, nome in urls:
        df_temp = buscar_dataframe(url, nome)
        if df_temp is not None:
            dfs_para_juntar.append(df_temp)
    
    if not dfs_para_juntar:
        # print("Nenhum dado foi retornado em nenhuma das requisições.")
        return None

    # print("\nIniciando a junção (merge) dos dados...")
    
    # Merge sequencial usando as chaves
    try:
        df_final = reduce(
            lambda left, right: pd.merge(left, right, on=['codmun', 'ano', 'mes'], how='outer'), 
            dfs_para_juntar
        )
    except Exception as e:
        # print(f"Erro ao juntar os DataFrames: {e}")
        return None

    data_referencia = datetime(year=ano_ref, month=mes_ref, day=1)
    # Cria uma coluna temporária de data para filtrar
    df_final['data_temp'] = pd.to_datetime(
        df_final['ano'].astype(str) + df_final['mes'].astype(str).str.zfill(2), 
        format='%Y%m'
    )
    df_final = df_final[df_final['data_temp'] > data_referencia]
    df_final = df_final.drop(columns=['data_temp'])
    
    if df_final.empty:
        # print(f"Nenhum dado novo após {mes_ref}/{ano_ref}")
        return None
    
    # Remove underscores dos nomes das colunas
    df_final.columns = df_final.columns.str.replace('_', '')

    cols_ignorar = ['codmun', 'ano', 'mes']
    
    # print("Convertendo colunas numéricas...")
    
    for col in df_final.columns:
        if col not in cols_ignorar:
            df_final[col] = pd.to_numeric(
                df_final[col].astype(str).str.replace(',', '.'), 
                errors='coerce'
            ).fillna(0).astype(int)
    
    # print("Adicionando colunas extras...")
    
    # Colunas de extrema pobreza com valor 0
    df_final['cadunicototpesextpob'] = 0
    df_final['cadunicototfamextpob'] = 0
    
    # Colunas de pobreza e extrema pobreza (cópia dos valores de pobreza)
    df_final['cadunicototfampobeextpob'] = df_final['cadunicototfampob']
    df_final['cadunicototpespobeextpob'] = df_final['cadunicototpespob']

    return df_final

def dataframe():
    mes, ano = get_ultimo_mes_ano(table_name)
    # print(f"Última data no banco: {mes}/{ano}")
    
    ano_atual = ano
    ano_soma = 0
    
    while True:
        ano_consulta = ano_atual + ano_soma
        # print(f"\n=== Consultando ano {ano_consulta} ===")
        
        df = consultar_cadunico_ano(ano_consulta, mes, ano)
        
        if df is not None and df.shape[0] > 0:
            # print(f"Adicionando {len(df)} novos registros...")
            add_values(df, table_name)
            # print(df)
            ano_soma += 1
        else:
            print(f"Sem dados novos para o ano {ano_consulta}. Finalizando inserções.")
            break # Sai do loop
    
    # Executa a query de update APÓS todas as inserções
    # print("\nExecutando atualização de chaves e siglas...")
    update_chaves_municipios(table_name)

def run_table_cadunico():
    try:
        dataframe()     
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")