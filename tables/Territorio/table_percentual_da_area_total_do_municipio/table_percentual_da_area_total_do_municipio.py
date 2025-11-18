import basedosdados as bd
import pandas as pd
from datetime import datetime
from utils.utils import get_municipio, add_values, get_ultimo_ano

table_name = "table_percentual_da_area_total_do_municipio"

def dataframe(ano):
    query = f"""
        SELECT
        ano,
        sigla_uf,
        id_municipio as codmun,
        id_classe,
        area
        FROM
        `basedosdados.br_mapbiomas_estatisticas.cobertura_municipio_classe`
        where ano = {ano};
    """


    df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')
    
    mapeamento_classe = {
        12: "Formação Campestre",
        49: "Restinga Arborizada",
        3: "Formação Florestal",
        15: "Pastagem",
        62: "Algodão (beta)",
        0: "outros",
        33: "Rio, Lago e Oceano",
        47: "Citrus",
        29: "Afloramento Rochoso",
        9: "Silvicultura",
        31: "Aquicultura",
        48: "Outras Lavouras Perenes",
        23: "Praia, Duna e Areal",
        21: "Mosaico de Usos",
        5: "Mangue",
        24: "Área Urbanizada",
        40: "Arroz",
        46: "Café",
        20: "Cana",
        30: "Mineração",
        4: "Formação Savânica",
        50: "Restinga Herbácea",
        39: "Soja",
        13: "Outras Formações não Florestais",
        11: "Campo Alagado e Área Pantanosa",
        25: "Outras Áreas não Vegetadas",
        41: "Outras Lavouras Temporárias",
        32: "Apicum"
    }

# Converter a coluna 'id_classe' para inteiro para evitar erros no mapeamento
    df['id_classe'] = df['id_classe'].astype(int)

    # Adicionar a coluna 'classe' com os valores correspondentes do dicionário de mapeamento
    df['classe'] = df['id_classe'].map(mapeamento_classe)

    # Se ainda houver NaN, substituir por "Desconhecido"
    df['classe'].fillna('Desconhecido', inplace=True)
    
    return df

def run_table_percentual_da_area_total_do_municipio():
    ultimo_ano = get_ultimo_ano(table_name)
    ano_atual  = datetime.now().year 
    mun = get_municipio()
    for ano in range(ultimo_ano+1, ano_atual):
        df = dataframe(ano)
        if df.shape[0]:
            df = pd.merge(df,mun, how='left')
            add_values(df,table_name)
