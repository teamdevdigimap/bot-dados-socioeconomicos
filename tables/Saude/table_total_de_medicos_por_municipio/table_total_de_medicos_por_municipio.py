from pysus.online_data.CNES import download
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
from utils.utils import get_municipio_codmun6, get_ultimo_mes_ano, add_values
import os
import logging

logging.basicConfig(level=logging.INFO)

'''
Ordem dos parâmetros de pysus.online.data.cnes.download:
{'group': 'Serviço Especializado',
 'last_update': '2023-02-17 07:31AM',
 'month': 'Janeiro',
 'name': 'SRSP2301.dbc',
 'size': '1.6 MB',
 'uf': 'São Paulo',
 'year': 2023}
 
 
 download(group: str, states: Union[str, list], years: Union[str, list, int], months: Union[str, list, int], data_dir: str = '/root/pysus') -> list
    Download CNES records for group, state, year and month and returns a
    list of local parquet files
    :param group:
        LT – Leitos - A partir de Out/2005
        ST – Estabelecimentos - A partir de Ago/2005
        DC - Dados Complementares - A partir de Ago/2005
        EQ – Equipamentos - A partir de Ago/2005
        SR - Serviço Especializado - A partir de Ago/2005
        HB – Habilitação - A partir de Mar/2007
        PF – Profissional - A partir de Ago/2005
        EP – Equipes - A partir de Abr/2007
        IN – Incentivos - A partir de Nov/2007
        RC - Regra Contratual - A partir de Mar/2007
        EE - Estabelecimento de Ensino - A partir de Mar/2007
        EF - Estabelecimento Filantrópico - A partir de Mar/2007
        GM - Gestão e Metas - A partir de Jun/2007
    :param months: 1 to 12, can be a list of years
    :param states: 2 letter state code, can be a list of UFs
    :param years: 4 digit integer, can be a list of years
'''
# help(download)
table_name = 'table_total_de_medicos_por_municipio'

data = {
    "CBO": [225151, 225225, 225125, 225142, 225130, 225250, 225124, 225133, 225355],
    "Médicos": [
        "Médico anestesiologista",
        "Médico cirurgião geral",
        "Médico clínico",
        "Médico da estratégia de saúde da família",
        "Médico de família e comunidade",
        "Médico ginecologista e obstetra",
        "Médico pediatra",
        "Médico psiquiatra",
        "Médico radiologista intervencionista"
    ]
}

# Criação do DataFrame
medicos = pd.DataFrame(data)


estados_brasil =[
    'AC',  # Acre
    'AL',  # Alagoas
    'AM',  # Amazonas
    'AP',  # Amapá
    'BA',  # Bahia
    'CE',  # Ceará
    'DF',  # Distrito Federal
    'ES',  # Espírito Santo
    'GO',  # Goiás
    'MA',  # Maranhão
    'MG',  # Minas Gerais
    'MS',  # Mato Grosso do Sul
    'MT',  # Mato Grosso
    'PA',  # Pará
    'PB',  # Paraíba
    'PE',  # Pernambuco
    'PI',  # Piauí
    'PR',  # Paraná
    'RJ',  # Rio de Janeiro
    'RN',  # Rio Grande do Norte
    'RO',  # Rondônia
    'RR',  # Roraima
    'RS',  # Rio Grande do Sul
    'SC',  # Santa Catarina
    'SE',  # Sergipe
    'SP',  # São Paulo
    'TO'   # Tocantins
]

def dataframe(estado, ano, mes):
    df = download('PF', estado, ano, mes).to_dataframe()
    #logging.info(f"Dados baixados para {estado} - {mes}/{ano}, total de registros: {df.shape[0]}")
    #logging.info(f"\nO dataframe gerado é {df.head()}")
    ids = ['225151', '225225', '225125', '225142', '225130', '225250', '225124', '225133', '225355']
    pd.set_option('display.max_columns', None)
    df_cbo = df[df['CBO'].isin(ids)]
    df_outros =  df[~df['CBO'].isin(ids)]
    #df_cbo = df_cbo[df_cbo['CODUFMUN'] == '330330']
    df_cbo = df_cbo[df_cbo['CPFUNICO'] == '1']
    df_cbo = df_cbo.drop_duplicates(subset=['CNS_PROF','CBO'])


    df_outros = df_outros[df_outros['CPFUNICO'] == '1']
    df_outros = df_outros.drop_duplicates(subset=['CNS_PROF','CBO'])
    counts_outros = df_outros.groupby(['CODUFMUN']).size().reset_index(name='Outras especialidades médicas')

    ids = ['225151', '225225', '225125', '225142', '225130', '225250', '225124', '225133', '225355']
    pd.set_option('display.max_columns', None)
    df_cbo = df[df['CBO'].isin(ids)]
    df_outros =  df[~df['CBO'].isin(ids)]
    #df_cbo = df_cbo[df_cbo['CODUFMUN'] == '330330']
    df_cbo = df_cbo[df_cbo['CPFUNICO'] == '1']
    df_cbo = df_cbo.drop_duplicates(subset=['CNS_PROF','CBO'])


    df_outros = df_outros[df_outros['CPFUNICO'] == '1']
    df_outros = df_outros.drop_duplicates(subset=['CNS_PROF','CBO'])
    counts_outros = df_outros.groupby(['CODUFMUN']).size().reset_index(name='Outras especialidades médicas')
    counts_outros
    counts = df_cbo.groupby(['CBO','CODUFMUN']).size().reset_index(name='Total')
    #counts = pd.merge(counts,medicos, on='CBO', how='left')
    counts['CBO'] = counts['CBO'].astype(str)
    medicos['CBO'] = medicos['CBO'].astype(str)
    counts = pd.merge(counts,medicos, on='CBO', how='left')
   
    url = "https://servicodados.ibge.gov.br/api/v3/agregados/4709/periodos/2022/variaveis/93?localidades=N6[all]"
    response = requests.get(url)
    retorno = response.text

    retorno = json.loads(retorno)

    codmun    = []
    populacao = []
    for r in retorno[0]['resultados'][0]['series']:
        codmun.append(r['localidade']['id'])
        populacao.append(r['serie']['2022'])

    populacao = pd.DataFrame({
        'codmun': codmun,
        'habitante': populacao
    })

    populacao['codmun'] = populacao['codmun'].str[:-1]

    counts = counts[['CODUFMUN', 'Médicos','Total']]
    counts.columns = ['codmun','medico','total']

    df = pd.merge(counts,populacao, how='left')

    # Tratar valores NaN
    df['habitante'] = pd.to_numeric(df['habitante'], errors='coerce').fillna(0).astype(int)
    df['total'] = df['total'].astype(int)
    
    # Evita divisão por zero
    df['medicos1000habitantes'] = df.apply(
        lambda row: (row['total'] / row['habitante']) * 1000 if row['habitante'] > 0 else 0, 
        axis=1
    )
    
    df['ano'] = ano
    df['mes'] = mes
    return df

def run_table_total_de_medicos_por_municipio():
    try:
        mun  = get_municipio_codmun6()
        mun.columns = ['codmun7', 'codmun','nome_sigla']
        mes,ano = get_ultimo_mes_ano(table_name)
        ultima_data = datetime(ano, mes, 1)
        ultima_data = ultima_data + relativedelta(months=1)
        data_atual = datetime.now()    
        while ultima_data < data_atual:
            mes = ultima_data.month
            ano = ultima_data.year
            
            for estado in estados_brasil:
                print(f"Atualizando mes {mes} ano {ano} Estado {estado}")
                df = dataframe(estado, ano=ano, mes=mes)
                if df.shape[0]:
                    df = pd.merge(df, mun, on='codmun', how='left')
                    add_values(df,table_name)
            
            ultima_data = ultima_data + relativedelta(months=1)       
            
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")      