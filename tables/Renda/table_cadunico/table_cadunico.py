import requests
import pandas as pd
from datetime import datetime
from utils.utils import add_values,get_ultimo_mes_ano
table_name = 'table_cadunico'

def dataframe():
    mes, ano = get_ultimo_mes_ano(table_name)
    # print(f"{mes}/{ano}")
    url_ano_atual = f"https://aplicacoes.mds.gov.br/sagi/servicos/misocial?fq=anomes_s:{ano}*&wt=csv&omitHeader=true&fq=cadunico_tot_fam_i:{{0%20TO%20*]&q=*&fl=ibge:codigo_ibge,anomes:anomes_s,cadunico_tot_fam:cadunico_tot_fam_i,cadunico_tot_pes:cadunico_tot_pes_i,cadunico_tot_fam_rpc_ate_meio_sm:cadunico_tot_fam_rpc_ate_meio_sm_i,cadunico_tot_pes_rpc_ate_meio_sm:cadunico_tot_pes_rpc_ate_meio_sm_i,cadunico_tot_fam_pob:cadunico_tot_fam_pob_i,cadunico_tot_pes_pob:cadunico_tot_pes_pob_i,cadunico_tot_fam_ext_pob:cadunico_tot_fam_ext_pob_i,cadunico_tot_pes_ext_pob:cadunico_tot_pes_ext_pob_i,cadunico_tot_fam_pob_e_ext_pob:cadunico_tot_fam_pob_e_ext_pob_i,cadunico_tot_pes_pob_e_ext_pob:cadunico_tot_pes_pob_e_ext_pob_i&rows=100000000&sort=anomes_s%20desc,%20codigo_ibge%20asc"
    response = requests.get(url_ano_atual)
    retorno = response.text.split("\n")
    ano_soma = 1
    while len(retorno) > 2:
        colunas = retorno[0].split(',')
        dados = [linha.split(',') for linha in retorno[1:]]
        # Criar o DataFrame usando as colunas e dados
        df = pd.DataFrame(dados, columns=colunas)
        df['anomes'] = pd.to_datetime(df['anomes'], format='%Y%m')
        data_referencia = datetime(year=ano, month=mes, day=1)
        df = df[df['anomes'] > f'{data_referencia}']
        df = df.rename(columns={'ibge':'codmun', 'anomes':'data'})
        df.columns = df.columns.str.replace('_', '')
        df['ano'] = df['data'].dt.year
        df['mes'] = df['data'].dt.month
        df = df.drop(columns=['data'])
        if df.shape[0]:
            # print("Adicionando novos dados...")
            for col in df.columns[1:]:
                df[col] = df[col].astype(int)
            
            add_values(df, table_name)

        url_ano_atual = f"https://aplicacoes.mds.gov.br/sagi/servicos/misocial?fq=anomes_s:{ano + ano_soma}*&wt=csv&omitHeader=true&fq=cadunico_tot_fam_i:{{0%20TO%20*]&q=*&fl=ibge:codigo_ibge,anomes:anomes_s,cadunico_tot_fam:cadunico_tot_fam_i,cadunico_tot_pes:cadunico_tot_pes_i,cadunico_tot_fam_rpc_ate_meio_sm:cadunico_tot_fam_rpc_ate_meio_sm_i,cadunico_tot_pes_rpc_ate_meio_sm:cadunico_tot_pes_rpc_ate_meio_sm_i,cadunico_tot_fam_pob:cadunico_tot_fam_pob_i,cadunico_tot_pes_pob:cadunico_tot_pes_pob_i,cadunico_tot_fam_ext_pob:cadunico_tot_fam_ext_pob_i,cadunico_tot_pes_ext_pob:cadunico_tot_pes_ext_pob_i,cadunico_tot_fam_pob_e_ext_pob:cadunico_tot_fam_pob_e_ext_pob_i,cadunico_tot_pes_pob_e_ext_pob:cadunico_tot_pes_pob_e_ext_pob_i&rows=100000000&sort=anomes_s%20desc,%20codigo_ibge%20asc"
        response = requests.get(url_ano_atual)
        retorno = response.text.split("\n")
        ano_soma = ano_soma + 1      


def run_table_cadunico():
    try:
        dataframe()    
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")    