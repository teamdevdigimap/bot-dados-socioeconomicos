import basedosdados as bd
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from utils.utils import get_municipio, add_values, get_ultimo_mes_ano

table_name = 'table_num_admitidos_desligados'

def dataframe():
    mun = get_municipio()
    mes, ano = get_ultimo_mes_ano(table_name)
    ultima_data = datetime(ano, mes, 1)
    ultima_data = ultima_data + relativedelta(months=1)
    data_atual = datetime.now() - relativedelta(months=1)
    while ultima_data < data_atual:
        ano = ultima_data.year
        mes = ultima_data.month
        # print(f"Ano {ano} -  Mes {mes}")
       
        query = f"""
        SELECT
            dados.ano AS ano,
            dados.mes AS mes,
            dados.id_municipio AS codmun,
            CASE 
                WHEN dados.tipo_movimentacao IN ('10', '20', '25', '35', '70') THEN 'Admissão'
                WHEN dados.tipo_movimentacao IN ('31', '32', '33', '40', '43', '45', '50', '60', '80', '90', '98') THEN 'Desligamento'
                ELSE 'Desconhecido'
            END AS descricaotipomovimentacao,
            COUNT(*) AS total
        FROM 
            `basedosdados.br_me_caged.microdados_movimentacao` AS dados
        WHERE 
            dados.tipo_movimentacao in ('10', '20', '25', '35', '70', '31', '32', '33', '40', '43', '45', '50', '60', '80', '90', '98')
            AND ano = {ano} and mes = {mes}
        GROUP BY
            dados.ano, 
            dados.mes, 
            dados.id_municipio, 
            descricaotipomovimentacao
        ORDER BY dados.id_municipio   
        """

        df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')

        if df.shape[0]:
            df = pd.merge(df, mun, how='left', on='codmun')
            add_values(df,table_name)
            
        ultima_data = ultima_data + relativedelta(months=1)            
        

def run_table_num_admitidos_desligados():
    try:
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")          