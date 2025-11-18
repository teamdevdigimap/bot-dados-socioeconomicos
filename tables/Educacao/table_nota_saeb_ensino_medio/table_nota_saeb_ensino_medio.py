import basedosdados as bd
from datetime import datetime
import pandas as pd
import psycopg2
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = 'table_nota_saeb_ensino_medio'

ultimo_ano = get_ultimo_ano(table_name) + 1
ano_atual = datetime.now().year + 1

def dataframe():
    for ano in range(ultimo_ano, ano_atual):
        #print("ano >>>>>>>", ano)
        query = f"""
        SELECT
            dados.id_municipio AS id_municipio,
            rede,
            ensino,
            nota_saeb_matematica,
            nota_saeb_lingua_portuguesa,
            nota_saeb_media_padronizada,
            ideb,
            anos_escolares,
            ano
        FROM `basedosdados.br_inep_ideb.municipio` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
            ON dados.id_municipio = diretorio_id_municipio.id_municipio
        WHERE ano = {ano}  and ensino = 'medio'  
        """

        df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')

        rename = {
            'id_municipio': 'codmun',
            'rede': 'Rede',
            'ensino': 'Ensino',
            'nota_saeb_matematica': 'Matemática',
            'nota_saeb_lingua_portuguesa': 'Língua Portuguesa',
            'nota_saeb_media_padronizada': 'Nota Média Padronizada (N)',
            'ideb': 'IDEB (N x P)',
            'ano': 'Ano'
        }

        df = df.rename(columns=rename)[['codmun', 'Rede', 'Matemática', 'Língua Portuguesa','Nota Média Padronizada (N)', 'IDEB (N x P)', 'Ano']]

        df.columns = [
            'codmun',
            'rede',
            'matematica',
            'linguaportuguesa',
            'notamediapadronizada',
            'ideb',
            'ano']
        mun = get_municipio()

        df = pd.merge(df, mun, on='codmun', how='left')
        if df.shape[0]:
            #print(df)
            add_values(df, table_name)

def run_table_nota_saeb_ensino_medio():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}") 