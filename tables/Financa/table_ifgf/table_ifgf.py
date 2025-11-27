from datetime import datetime
import basedosdados as bd
import requests
import json
import pandas as pd
from utils.utils import add_values, get_municipio, get_ultimo_ano, get_municipio_codmun6
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_ifgf'
# Obter o último ano registrado
ultimo_ano = get_ultimo_ano(table_name)
data_atual = datetime.now().year

# print(f"Último ano na tabela: {ultimo_ano}")
# print(f"Ano atual: {data_atual}")


df_municipios = get_municipio()

#função para inserção manual de dados a partir de um arquivo CSV
#https://firjan.com.br/ifgf/analises-e-rankings/?_gl=1*xtd244*_gcl_au*MTcyMTU4NjA3MS4xNzM0NTQzOTU4
def insert_manual_ifgf_from_csv(file_path, sep=';'):
    """
    Lê um arquivo CSV contendo dados do IFGF e insere no banco.
    
    Args:
        file_path (str): Caminho completo para o arquivo .csv
        sep (str): Separador do CSV. Padrão é ';' (comum no Brasil quando decimais usam vírgula).
                   Se o seu arquivo usar tabulação, mude para sep='\t'.
    """
    print(f"\n---> Processando arquivo manual: {file_path} ---")

    try:
        # 1. Leitura do arquivo
        df = pd.read_csv(file_path, sep=sep, dtype=str)
        
        # Validação básica de colunas
        if 'Código' not in df.columns:
            raise Exception("O CSV deve conter a coluna 'Código' (IBGE 6 dígitos).")

        # 2. Identificar colunas de Ano (melt/unpivot)
        cols_fixas = ['Código', 'UF', 'Município']
        cols_anos = [c for c in df.columns if c not in cols_fixas]
        
        print(f"Anos identificados no arquivo: {cols_anos}")

        df_melted = pd.melt(
            df,
            id_vars=cols_fixas,
            value_vars=cols_anos,
            var_name='ano',
            value_name='ifgf_raw'
        )

        df_melted['ifgf'] = df_melted['ifgf_raw'].str.replace(',', '.', regex=False)
        
        df_melted['ifgf'] = pd.to_numeric(df_melted['ifgf'], errors='coerce')
        
        df_melted['ano'] = df_melted['ano'].astype(int)

        # 4. Cruzamento com Tabela de Municípios (Utils)
        df_mun_full = get_municipio_codmun6()
        
        # Garantir tipagem para o merge
        df_mun_full['codmun6'] = df_mun_full['codmun6'].astype(str)
        df_melted['Código'] = df_melted['Código'].astype(str)

        df_final = df_melted.merge(
            df_mun_full[['codmun', 'codmun6', 'nome_sigla']], 
            left_on='Código', 
            right_on='codmun6', 
            how='left'
        )

        # 5. Preparação final para o add_values
        df_insert = pd.DataFrame()
        df_insert['codmun'] = df_final['codmun']        # Código 7 dígitos
        df_insert['ifgf'] = df_final['ifgf']            # Valor float
        df_insert['ano'] = df_final['ano']              # Ano integer
        df_insert['nome_sigla'] = df_final['nome_sigla']
        df_insert['cod_mun7'] = df_final['codmun']      # Redundância solicitada na estrutura
        
        # Filtrar apenas os que conseguiram match de município
        n_antes = len(df_insert)
        df_insert = df_insert.dropna(subset=['codmun'])
        n_depois = len(df_insert)
        
        if n_antes > n_depois:
            print(f"Aviso: {n_antes - n_depois} registros foram ignorados pois o Código IBGE (6 dígitos) não foi encontrado no banco.")

        # 6. Inserção
        if not df_insert.empty:
            print(f"Inserindo {len(df_insert)} registros...")
            add_values(df_insert, table_name)
        else:
            print("Nenhum dado válido para inserir.")

    except FileNotFoundError:
        print(f"Erro: O arquivo '{file_path}' não foi encontrado.")
    except Exception as e:
        print(f"Erro crítico ao processar o arquivo: {e}")


def dataframe():
    # Verificar necessidade de atualização
    for ano in range(ultimo_ano + 1, data_atual+1):
        try:
            #ultimo_ano 
            # print(">>>>>>>>>>>>>>>>>>> ANO", ultimo_ano)
            # print(">>>>>>>>>>>>>>>>>>> ANO ATUAL", data_atual)
            
            # Consulta os dados da API do Base dos Dados
            query = f"""
            SELECT
                id_municipio AS codmun,  
                indice_firjan_gestao_fiscal AS ifgf,
                sigla_uf,
                ranking_estadual,
                ranking_nacional,
                ano
            FROM 
                `basedosdados.br_firjan_ifgf.ranking` 
            WHERE 
                ano = {ano};
            """

            df = bd.read_sql(query, billing_project_id=os.environ['USER'])
            # print(f"Dados obtidos para o ano {ano}\n", df.head(5))

            # Adicionando os nomes dos municípios com merge
            df = df.merge(df_municipios, on='codmun', how='left')

            # Adicionando a data de criação dos registros

            # Inserindo os dados no banco de dados
            df = df.rename(columns={"codmun": "cod_mun7"})
            df = df[["ifgf","ano", "nome_sigla", "cod_mun7"]]
            if df.shape[0]:
                # print('o dataframe é: \n', df)
                add_values(df, table_name)

        except Exception as error:
            print(f"Erro durante a atualização da tabela: {error}")

    # else:
    #     print(f"A tabela {table_name} já está atualizada.")


def run_table_ifgf():
    try:
        dataframe()
        
        #adicionar aqui o caminho do csv manual caso haja necessidade de inserção manual de dados
        # caminho_csv = "" 
        caminho_csv = None

        if caminho_csv is not None:
            try: 
                if os.path.exists(caminho_csv):
                    insert_manual_ifgf_from_csv(caminho_csv, sep=';')
                else:
                    print(f"Arquivo CSV manual não encontrado em: {caminho_csv}.")
            except Exception as e:
                print(f"Erro ao inserir dados manuais do CSV: {e}")
        elif caminho_csv is None:
            print("Nenhum arquivo CSV manual fornecido para inserção.")
        
            
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")