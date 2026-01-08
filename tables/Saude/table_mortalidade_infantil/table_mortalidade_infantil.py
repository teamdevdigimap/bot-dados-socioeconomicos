import pandas as pd
from datetime import datetime
import numpy as np
from utils.utils import get_municipio_codmun6, get_ultimo_ano, add_values, update_chaves_municipios
from pysus.online_data.SIM import download

table_name = 'table_mortalidade_infantil'

def get_chapter(cid):
    if pd.isna(cid):
        return None
    cid = str(cid).strip().upper()[:3]
    chapters = [
        ('A00', 'B99', 'Certain infectious and parasitic diseases'),
        ('C00', 'D48', 'Neoplasms'),
        ('D50', 'D89', 'Diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism'),
        ('E00', 'E90', 'Endocrine, nutritional and metabolic diseases'),
        ('F00', 'F99', 'Mental and behavioural disorders'),
        ('G00', 'G99', 'Diseases of the nervous system'),
        ('H00', 'H59', 'Diseases of the eye and adnexa'),
        ('H60', 'H95', 'Diseases of the ear and mastoid process'),
        ('I00', 'I99', 'Diseases of the circulatory system'),
        ('J00', 'J99', 'Diseases of the respiratory system'),
        ('K00', 'K93', 'Diseases of the digestive system'),
        ('L00', 'L99', 'Diseases of the skin and subcutaneous tissue'),
        ('M00', 'M99', 'Diseases of the musculoskeletal system and connective tissue'),
        ('N00', 'N99', 'Diseases of the genitourinary system'),
        ('O00', 'O99', 'Pregnancy, childbirth and the puerperium'),
        ('P00', 'P96', 'Certain conditions originating in the perinatal period'),
        ('Q00', 'Q99', 'Congenital malformations, deformations and chromosomal abnormalities'),
        ('R00', 'R99', 'Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified'),
        ('S00', 'T98', 'Injury, poisoning and certain other consequences of external causes'),
        ('V01', 'Y98', 'External causes of morbidity and mortality'),
        ('Z00', 'Z99', 'Factors influencing health status and contact with health services'),
        ('U00', 'U99', 'Codes for special purposes')
    ]
    for start, end, desc in chapters:
        if start <= cid <= end:
            return desc
    return 'Unknown'

def dataframe(ano):
    df_all = pd.DataFrame()
    estados_brasil = [
        'AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
        'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'
    ]
    
    for uf in estados_brasil:
        print(f'Processando {uf} para o ano {ano}')
        try:
            # 1. Baixa o objeto (pode ser um DataFrame ou um ParquetSet)
            resultado_download = download(states=[uf], years=[ano], groups=['CID10'])
            
            df = None
            
            # 2. Verifica se é um ParquetSet e converte para DataFrame
            if hasattr(resultado_download, 'to_dataframe'):
                df = resultado_download.to_dataframe()
            elif isinstance(resultado_download, pd.DataFrame):
                df = resultado_download
            else:
                print(f" > Aviso: Formato de retorno desconhecido para {uf} {ano}")
                continue

            # 3. Prossegue com a lógica normal
            if df is not None and not df.empty:
                # Normaliza colunas para maiúsculo (o SIM varia muito isso)
                df.columns = [c.upper() for c in df.columns]

                if 'IDADE' in df.columns:
                    # Filtra e converte IDADE
                    df['IDADE'] = df['IDADE'].astype(str).str.zfill(3)
                    # O replace trata casos vazios ou nulos que podem quebrar a conversão
                    df['IDADE_unit'] = df['IDADE'].str[0].replace('', '9').replace('nan', '9').astype(int)
                    
                    # Filtra mortalidade infantil (unidade de tempo < 4: horas, dias, meses)
                    df_infant = df[df['IDADE_unit'] < 4].copy()
                    
                    if not df_infant.empty:
                        # Pega o capítulo do CID
                        df_infant['causa'] = df_infant['CAUSABAS'].apply(get_chapter)
                        
                        # Agrupa
                        group = df_infant.groupby(['CODMUNRES', 'causa']).size().reset_index(name='obitos')
                        group['ano'] = ano
                        group['codmun'] = group['CODMUNRES'].astype(str)
                        
                        df_all = pd.concat([df_all, group])
                        print(f" > Sucesso: {uf} {ano} ({len(group)} registros agrupados)")
                    else:
                        print(f" > {uf} {ano}: Nenhum óbito infantil encontrado nos dados.")
                else:
                    print(f" > Aviso: {uf} {ano} sem coluna IDADE. Colunas: {df.columns}")
            else:
                print(f" > Vazio: {uf} {ano}")
                
        except Exception as e:
            print(f"Info: Não foi possível processar {uf} {ano}. Erro: {e}")
            
    if not df_all.empty:
        return df_all[['ano', 'codmun', 'causa', 'obitos']]
        
    return pd.DataFrame()

def run_table_mortalidade_infantil():
    try:
        mun = get_municipio_codmun6()
        try:
            ultimo_ano = get_ultimo_ano(table_name)
            if ultimo_ano is None: ultimo_ano = 2009
        except:
            ultimo_ano = 2009
        
        ano_inicial = ultimo_ano + 1
        ano_atual = datetime.now().year 
        
        for ano in range(ano_inicial, ano_atual + 1):
            print(f"--- Iniciando consolidação do ano {ano} ---")
            df = dataframe(ano)
            
            if not df.empty:
                mun['codmun6'] = mun['codmun6'].astype(str)
                df['codmun'] = df['codmun'].astype(str)

                # Merge para pegar nome_sigla (metadados)
                df = pd.merge(df, mun[['codmun6', 'nome_sigla']], left_on='codmun', right_on='codmun6', how='left')
                
                if 'codmun6' in df.columns:
                    df = df.drop('codmun6', axis=1)
                
                # Tratamento de NaNs gerados pelo merge (municípios novos ou extintos)
                df['nome_sigla'] = df['nome_sigla'].fillna('Desconhecido')
                
                print(f"Inserindo {len(df)} registros no banco para o ano {ano}...")
                add_values(df, table_name)
                update_chaves_municipios(table_name)
            else:
                print(f"Sem dados consolidados para o ano {ano}")
                
    except Exception as e:
        print(f"Erro crítico na execução geral: {e}")