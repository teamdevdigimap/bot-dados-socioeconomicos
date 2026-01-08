import basedosdados as bd
import pandas as pd
import os
from dotenv import load_dotenv
from utils.utils import add_values, get_municipio, update_chaves_municipios

# Carrega variáveis de ambiente (para autenticação do Google Cloud/Base dos Dados)
load_dotenv()

table_name = 'table_domicilios_particulares_censo_2022'

def dataframe():
    """
    Busca os dados de domicílios particulares do Censo 2022 no BigQuery.
    """
    # Query fornecida, adaptada para retornar apenas o necessário para o Python processar
    # Removemos o JOIN com diretórios aqui, pois faremos o merge com a tabela local de municípios
    # para garantir consistência com o banco de dados local.
    query = """
    SELECT
        dados.id_municipio AS codmun,
        SUM(dados.domicilios) AS total_domicilios_particulares
    FROM `basedosdados.br_ibge_censo_2022.domicilio_recenseado` AS dados
    WHERE dados.especie NOT IN ('Coletivo', 'Coletivo - sem morador', 'Coletivo - com morador')
    GROUP BY dados.id_municipio
    """
    
    print(f"Executando query no BigQuery para {table_name}...")
    
    # Billing_project_id deve estar configurado no .env como 'USER' ou o ID do projeto GCP
    df = bd.read_sql(query, billing_project_id=os.environ['USER'])
    
    if not df.empty:
        # Garantir tipos de dados corretos
        df['codmun'] = df['codmun'].astype(str)
        df['total_domicilios_particulares'] = df['total_domicilios_particulares'].astype(int)
        
        return df
    
    return pd.DataFrame()

def run_table_domicilios_particulares_censo_2022():
    """
    Função principal para orquestrar a atualização da tabela.
    """
    try:
        # 1. Carregar dados do BigQuery
        df = dataframe()
        
        if not df.empty:
            # 2. Obter dados auxiliares de municípios (para preencher nome_sigla)
            mun = get_municipio()
            
            # Garantir que a chave de junção seja string em ambos
            mun['codmun'] = mun['codmun'].astype(str)
            
            # 3. Cruzar dados para obter nome_sigla
            # O how='left' garante que mantemos os dados do censo mesmo se faltar metadata (raro)
            df = pd.merge(df, mun, how='left', on='codmun')
            
            # Selecionar apenas as colunas existentes na tabela de destino
            # (codmun, total_domicilios_particulares, nome_sigla)
            cols_to_keep = ['codmun', 'total_domicilios_particulares', 'nome_sigla']
            df = df[cols_to_keep]
            
            print(f"Inserindo {len(df)} registros na tabela {table_name}...")
            
            # 4. Inserir no banco de dados
            add_values(df, table_name)
            
            # 5. Garantir atualização de chaves/metadados se necessário
            update_chaves_municipios(table_name)
            
            print(f"Processo finalizado com sucesso para {table_name}.")
        else:
            print("A consulta não retornou dados.")
            
    except Exception as e:
        print(f"Erro ao atualizar a tabela {table_name}:\n{e}")
