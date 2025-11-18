from pysus.online_data.CNES import download
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
from utils.utils import get_municipio_codmun6, get_ultimo_mes_ano, add_values

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
    df = download(state=estado, year=ano, month=mes, group='PF')
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

    df['habitante'] = df['habitante'].astype(int)
    df['total'] = df['total'].astype(int) 
    df['medicos1000habitantes'] = (df['total'] / df['habitante']) * 1000
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