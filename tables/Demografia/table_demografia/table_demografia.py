import basedosdados as bd
from datetime import datetime
from utils.utils import add_values, get_ultimo_ano, get_municipio
import os
from dotenv import load_dotenv

load_dotenv()
table_name = "table_demografia"

def dataframe():
    ultimo_ano = get_ultimo_ano(table_name)
    data_atual = datetime.now().year

    print(f"Último ano na tabela: {ultimo_ano}")
    print(f"Ano atual: {data_atual}")

    # Verificar necessidade de atualização
    for ano in range(ultimo_ano+1, data_atual+1):
        #ultimo_ano += 1
        query = f"""
        SELECT
            id_municipio AS codmun,
            expectativa_vida AS espvida,
            fecundidade_total AS fectot,
            mortalidade_1 AS mort1,
            mortalidade_5 AS mort5,
            razao_dependencia AS razdep,
            populacao_homens_0_4 AS homem0a4,
            populacao_homens_10_14 AS homem10a14,
            populacao_homens_15_19 AS homem15a19,
            populacao_homens_20_24 AS homem20a24,
            populacao_homens_25_29 AS homem25a29,
            populacao_homens_30_34 AS homem30a34,
            populacao_homens_35_39 AS homem35a39,
            populacao_homens_40_44 AS homem40a44,
            populacao_homens_45_49 AS homem45a49,
            populacao_homens_50_54 AS homem50a54,
            populacao_homens_55_59 AS homem55a59,
            populacao_homens_5_9 AS homem5a9,
            populacao_homens_60_64 AS homem60a64,
            populacao_homens_65_69 AS homem65a69,
            populacao_homens_70_74 AS homem70a74,
            populacao_homens_75_79 AS homem75a79,
            populacao_homens_80_mais AS homens80,
            populacao_homens AS homemtot,
            populacao_mulheres_0_4 AS mulh0a4,
            populacao_mulheres_10_14 AS mulh10a14,
            populacao_mulheres_15_19 AS mulh15a19,
            populacao_mulheres_20_24 AS mulh20a24,
            populacao_mulheres_25_29 AS mulh25a29,
            populacao_mulheres_30_34 AS mulh30a34,
            populacao_mulheres_35_39 AS mulh35a39,
            populacao_mulheres_40_44 AS mulh40a44,
            populacao_mulheres_45_49 AS mulh45a49,
            populacao_mulheres_50_54 AS mulh50a54,
            populacao_mulheres_55_59 AS mulh55a59,
            populacao_mulheres_5_9 AS mulh5a9,
            populacao_mulheres_60_64 AS mulh60a64,
            populacao_mulheres_65_69 AS mulh65a69,
            populacao_mulheres_70_74 AS mulh70a74,
            populacao_mulheres_75_79 AS mulh75a79,
            populacao_mulheres_80_mais AS mulher80,
            populacao_mulheres AS mulhertot,
            populacao AS pop,
            ano
        FROM
            `basedosdados.mundo_onu_adh.municipio`
        WHERE
            ano = {ano};
        """

        df = bd.read_sql(query, billing_project_id=os.environ['USER'])
        df_municipios = get_municipio()
        df = df.merge(df_municipios, on='codmun', how='left')
        if df.shape[0]:
            #print(df)   
            add_values(df,table_name)

def run_table_demografia():
    try:
        dataframe()    
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")  