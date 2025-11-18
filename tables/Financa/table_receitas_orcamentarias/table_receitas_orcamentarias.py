import basedosdados as bd
from datetime import datetime
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = 'table_receitas_orcamentarias'
ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1

# print(table_name)

def dataframe():
    try:
        for ano in range(ultimo_ano, ano_atual):

            query = f"""
            SELECT
                dados.id_municipio AS id_municipio,
                estagio, 
                conta as Conta,
                valor as Valor, 
                ano
            FROM `basedosdados.br_me_siconfi.municipio_receitas_orcamentarias` AS dados
            LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
                ON dados.id_municipio = diretorio_id_municipio.id_municipio
                WHERE ano = {ano} 
            """



            df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')
            df = df.rename(columns={'id_municipio':'codmun', 'estagio': 'Deduções'})
            df.columns = ['codmun', 'deducoes', 'conta', 'valor', 'ano']
            df_municipios = get_municipio()
            df = df.merge(df_municipios, on='codmun', how='left')
            if df.shape[0]:
                #print(df)
                add_values(df,table_name)
    except Exception as e:
            print(f"Erro para atualizar tabela {table_name}\nErro: {e}")  

def run_table_receitas_orcamentarias():
    try:
        dataframe()    
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")  

