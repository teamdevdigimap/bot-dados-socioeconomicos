import basedosdados as bd
from datetime import datetime
import pandas as pd
from utils.utils import add_values, get_municipio, get_ultimo_ano
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'taxa_de_leitos_de_internecao'

def dataframe(ano):
    query = f""" 
    SELECT
        populacao.ano AS ano,
        populacao.codmun,
        --populacao.populacao,
        --leitos.totalleitos,
        -- Calcular a taxa de leitos por habitante
        (leitos.totalleitos / populacao.populacao) AS taxadeleitos
    FROM 
        (SELECT
            dados.ano AS ano,
            dados.id_municipio AS codmun,
            dados.populacao AS populacao
        FROM `basedosdados.br_ibge_populacao.municipio` AS dados
        WHERE ano = {ano}) AS populacao
    JOIN 
        (SELECT
            dados.ano AS ano,
            dados.id_municipio AS codmun,
            SUM(dados.quantidade_leito_cirurgico +
                dados.quantidade_leito_clinico +
                dados.quantidade_leito_complementar +
                dados.quantidade_leito_repouso_pediatrico_urgencia +
                dados.quantidade_leito_repouso_feminino_urgencia +
                dados.quantidade_leito_repouso_masculino_urgencia +
                dados.quantidade_leito_repouso_indiferenciado_urgencia +
                dados.quantidade_leito_repouso_feminino_ambulatorial +
                dados.quantidade_leito_repouso_masculino_ambulatorial +
                dados.quantidade_leito_repouso_pediatrico_ambulatorial +
                dados.quantidade_leito_repouso_indiferenciado_ambulatorial +
                dados.quantidade_leito_recuperacao_centro_cirurgico +
                dados.quantidade_leito_pre_parto_centro_obstetrico +
                dados.quantidade_leito_recem_nascido_normal_neonatal +
                dados.quantidade_leito_recem_nascido_patologico_neonatal +
                dados.quantidade_leito_conjunto_neonatal) AS totalleitos
        FROM `basedosdados.br_ms_cnes.estabelecimento` AS dados
        WHERE dados.ano = {ano}
        GROUP BY dados.ano, dados.id_municipio) AS leitos
    ON populacao.codmun = leitos.codmun;
    """

    df = bd.read_sql(query, billing_project_id=os.environ['USER'])
    return df

def run_taxa_de_leitos_de_internecao():
    try:
        mun = get_municipio()
        ultimo_ano = get_ultimo_ano(table_name)
        ano_atual = datetime.now().year
        for ano in range(ultimo_ano+1, ano_atual+1):
             df = dataframe(ano)
             if df.shape[0]:
                df = pd.merge(df,mun,how='left', on='codmun')
                add_values(df,table_name)
    
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")     