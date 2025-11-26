import pandas as pd
from utils.utils import add_values, get_ultimo_mes_ano, get_municipio

# Nome da tabela no banco de dados
table_name = 'table_censo_suas'

# Dicionário para mapear os meses para seus períodos
mes_periodo = {
    1: 'primeiro',  2: 'primeiro',  3: 'primeiro',  4: 'primeiro', 5: 'primeiro',  6: 'primeiro',  
    7: 'segundo',   8: 'segundo',  9: 'segundo',  10: 'segundo',  11: 'segundo',  12: 'segundo'
}

def dataframe():
    """ Função responsável por baixar, processar e formatar os dados para inserção no banco. """
    try:
        # Obtém o último ano e mês registrado no banco
        mes, ano = get_ultimo_mes_ano(table_name)

        # Definição manual para depuração (remova essa linha para usar o banco)
        #ano = 2023
        #mes = 12

        # Determina o período com base no mês
        periodo = mes_periodo[mes]

        # Determina o próximo período
        if periodo == "primeiro":
            periodo = "segundo"
        elif periodo == "segundo":
            periodo = "primeiro"
            ano += 1  # Avança para o próximo ano

        # Gera o link do dataset para o período e ano atualizados
        link = f'https://dadosabertos.mdh.gov.br/disque100-{periodo}-semestre-{ano}.csv'
        print(f"Baixando dados do link: {link}")

        # Baixa e carrega o CSV
        df = pd.read_csv(link, sep=';')

        # Adiciona a coluna do período
        df['periodo'] = periodo

        # Seleciona apenas as colunas relevantes
        df = df[['Município', 'Data_de_cadastro', 'violacao', 'periodo']]

        # Filtra ocorrências de "direitos humanos"
        df = df[df['violacao'].str.contains('direitos humanos', case=False, na=False)]

        # Converte a data para datetime
        df['Data_de_cadastro'] = pd.to_datetime(df['Data_de_cadastro'], errors='coerce')

        # Extrai o ano e o mês
        df['ano'] = df['Data_de_cadastro'].dt.year
        df['mes'] = df['Data_de_cadastro'].dt.month

        # Extrai o código do município corretamente
        df['codmun'] = df['Município'].str.split("|", expand=True)[0]

        # Mantém apenas as colunas necessárias
        df = df[['codmun', 'ano', 'mes', 'violacao', 'periodo']]

        # Agrupa os dados por município, ano, mês e período
        df = df.groupby(['codmun', 'ano', 'mes', 'periodo']).size().reset_index(name='total')

        # Converte codmun para numérico e remove entradas inválidas
        df['codmun'] = pd.to_numeric(df['codmun'], errors='coerce')
        df = df.dropna(subset=['codmun'])
        df['codmun'] = df['codmun'].astype(int)

        return df

    except Exception as e:
        raise Exception(f"Erro no processamento dos dados: {e}")

def run_table_censo_suas():
    """ Função para executar o fluxo de atualização da tabela no banco de dados. """
    try:
        df = dataframe()

        if df.shape[0]:
            mun = get_municipio()
            #print("Inserindo novos dados no banco...")
            df['codmun'] = df['codmun'].astype(str) 
            df = pd.merge(df,mun,how='left', on='codmun')
            print(df)
            
            add_values(df, table_name)
            print("Dados atualizados com sucesso!")
        else:
            print("Nenhum novo dado para inserir.")

    except Exception as e:
        print(f"Erro ao atualizar tabela {table_name}. Detalhes: {e}")
