import ssl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
import pandas as pd
from utils.utils import add_values, get_municipio, get_ultimo_mes_ano

ssl._create_default_https_context = ssl._create_unverified_context

table_name = "table_arrecadacao_cefem"
import ssl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
import pandas as pd
from utils.utils import add_values, get_municipio, get_ultimo_mes_ano

ssl._create_default_https_context = ssl._create_unverified_context

table_name = "table_arrecadacao_cefem"

mun = get_municipio()
mes_atual, ano_atual = get_ultimo_mes_ano(table_name)
# print(table_name)
# print(f"ultimo mes e ano>>>>>>>>>>>>{mes_atual}, {ano_atual}")

def dataframe():
    try:
        url = "https://app.anm.gov.br/DadosAbertos/ARRECADACAO/CFEM_Arrecadacao.csv"

        df = pd.read_csv(url, encoding='ISO-8859-1', delimiter=',')
        #print(df)
        df = df.rename(columns={'CodigoMunicipio': 'codmun'})
        df = df[['codmun', 'ValorRecolhido', 'Mês', 'Ano']]
        df.columns = ['codmun', 'valorrecolhido', 'mes', 'ano']
        df['valorrecolhido'] = df['valorrecolhido'].astype(str).str.replace(",", ".").astype(float)
        
        df['codmun'] = df['codmun'].astype(str)
        mun['codmun'] = mun['codmun'].astype(str)
        # Verificando valores únicos na coluna "Mês"
        df["ano"] = df["ano"].astype(int)
        df["mes"] = df["mes"].astype(int)

        ultima_data = datetime(ano_atual, mes_atual, 1) + relativedelta(months=1)
        data_atual = datetime.now()
        
        while ultima_data < data_atual:
            mes = ultima_data.month
            ano = ultima_data.year
            print(f"Atualizando mês {mes} ano {ano}")
            atualizacao = df[(df['mes'] == mes) & (df['ano'] == ano)]
            if not atualizacao.empty:
                atualizacao = atualizacao.merge(mun, on='codmun', how='left')
                #print(atualizacao)

                add_values(atualizacao, table_name)
            ultima_data = ultima_data + relativedelta(months=1)
    except Exception as e:
        print(f"Erro ao atualizar tabela {table_name}\nErro: {e}")

def run_table_arrecadacao_cefem():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")
