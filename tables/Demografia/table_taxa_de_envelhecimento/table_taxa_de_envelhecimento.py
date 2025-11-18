import basedosdados as bd
import psycopg2
from datetime import datetime
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = "table_taxa_de_envelhecimento"

def dataframe():
    #print("dataframe entrou>>>>>>>>>>>>>>>>>>>>>>>>>")
    ultimo_ano = get_ultimo_ano(table_name)
    data_atual = datetime.now().year

    # Verificar necessidade de atualização
    #if ultimo_ano < data_atual:
    for ano in range(ultimo_ano+1, data_atual+1):
        query = f"""
        SELECT
            dados.ano as ano,
            dados.id_municipio AS codmun,
            dados.indice_envelhecimento as indice_envelhecimento,
            dados.idade_mediana as idade_mediana,
            dados.razao_sexo as razao_sexo
        FROM `basedosdados.br_ibge_censo_2022.indice_envelhecimento_raca` AS dados
        WHERE dados.ano = {ano}
        """

        df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')

        # Renomeando as colunas conforme a estrutura da tabela no banco de dados
        df.rename(columns={
            'indice_envelhecimento': 'indice_envelhecimento',
            'idade_mediana': 'idade_mediana',
            'razao_sexo': 'razao_sexo'
        }, inplace=True)


        # Obtendo os nomes dos municípios e associando ao DataFrame
        df_municipios = get_municipio()

        df = df.merge(df_municipios, on='codmun', how='left')
        df = df.dropna()
        # Inserindo os dados no banco de dados
        if df.shape[0]:
            #print(df)
            add_values(df, table_name)


def run_table_taxa_de_envelhecimento():
    try:
        #print(">>>>>>>>>>>>>>>table_taxa_de_envelhecimento", table_name)
        dataframe()    
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")  