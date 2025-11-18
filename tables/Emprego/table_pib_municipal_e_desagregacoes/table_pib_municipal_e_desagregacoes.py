import datetime
import basedosdados as bd
import numpy as np
import pandas as pd
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = 'table_pib_municipal_e_desagregacoes'



def dataframe(ano):
    query = f"""
    SELECT
        dados.id_municipio AS id_municipio,
        pib,
        impostos_liquidos,
        va,
        va_agropecuaria,
        va_industria,
        va_servicos,
        va_adespss,
        ano
    FROM `basedosdados.br_ibge_pib.municipio` AS dados
    LEFT JOIN (SELECT DISTINCT id_municipio, nome FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
        ON dados.id_municipio = diretorio_id_municipio.id_municipio
        WHERE ano = {ano}
    """
    
    df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')
    if df.shape[0]:
        # Renomear as colunas
        df.columns = [
            'codmun', 
            'produto_interno_bruto_a_precos_correntes_mil_reais',
            'impostos_liquidos_de_subsidios_sobre_produtos_a_precos_cor',
            'valor_adicionado_bruto_a_precos_correntes_total_mil_reais',
            'valor_adicionado_bruto_a_precos_correntes_da_agropecuaria_mi',
            'valor_adicionado_bruto_a_precos_correntes_da_industria_mil_r',
            'valor_adicionado_bruto_a_precos_correntes_dos_servicos_exclu',
            'valor_adicionado_bruto_a_precos_correntes_da_administracao_',
            'ano'
        ]
        
        # Converter tipos de dados
        df['codmun'] = df['codmun'].astype(str)
        df['produto_interno_bruto_a_precos_correntes_mil_reais'] = df['produto_interno_bruto_a_precos_correntes_mil_reais'].astype(float)
        df['impostos_liquidos_de_subsidios_sobre_produtos_a_precos_cor'] = df['impostos_liquidos_de_subsidios_sobre_produtos_a_precos_cor'].astype(float)
        df['valor_adicionado_bruto_a_precos_correntes_total_mil_reais'] = df['valor_adicionado_bruto_a_precos_correntes_total_mil_reais'].astype(float)
        df['valor_adicionado_bruto_a_precos_correntes_da_agropecuaria_mi'] = df['valor_adicionado_bruto_a_precos_correntes_da_agropecuaria_mi'].astype(float)
        df['valor_adicionado_bruto_a_precos_correntes_da_industria_mil_r'] = df['valor_adicionado_bruto_a_precos_correntes_da_industria_mil_r'].astype(float)
        df['valor_adicionado_bruto_a_precos_correntes_dos_servicos_exclu'] = df['valor_adicionado_bruto_a_precos_correntes_dos_servicos_exclu'].astype(float)
        df['valor_adicionado_bruto_a_precos_correntes_da_administracao_'] = df['valor_adicionado_bruto_a_precos_correntes_da_administracao_'].astype(float)
        df['ano'] = df['ano'].astype(int)

        return(df)
    

    return np.array([])    


def run_table_pib_municipal_e_desagregacoes():
    try:
        mun = get_municipio()
        ultimo_ano = get_ultimo_ano(table_name)
        ano_atual = datetime.datetime.now().year
        for ano in range(ultimo_ano+1, ano_atual+1):
            df = dataframe(ano)
            if df.shape[0]:
                df = pd.merge(df,mun, how='left', on='codmun')
                add_values(df,table_name)
        
        
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")   
        