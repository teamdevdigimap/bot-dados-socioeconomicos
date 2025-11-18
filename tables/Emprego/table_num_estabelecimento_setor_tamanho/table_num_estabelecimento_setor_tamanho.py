import basedosdados as bd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from utils.utils import add_values, get_ultimo_mes_ano, get_municipio

table_name = "table_num_estabelecimento_setor_tamanho"


def dataframe(ano,mes):
    query = f"""
        SELECT
        dados.ano,
        dados.mes,
        dados.id_municipio,
        diretorio_id_municipio.nome AS id_municipio_nome,
        tamanho_estabelecimento_janeiro,
        CASE
            WHEN dados.cnae_2_subclasse LIKE '05%' OR dados.cnae_2_subclasse LIKE '07%' OR dados.cnae_2_subclasse LIKE '08%' THEN 'Extrativa mineral'
            WHEN dados.cnae_2_secao = 'C' THEN 'Indústria de transformação'
            WHEN dados.cnae_2_subclasse LIKE '352%' OR dados.cnae_2_subclasse LIKE '351%' OR dados.cnae_2_subclasse LIKE '36%' OR dados.cnae_2_subclasse LIKE '37%' OR dados.cnae_2_subclasse LIKE '38%' THEN 'Serviços industriais de utilidade pública'
            WHEN dados.cnae_2_secao = 'F' THEN 'Construção Civil'
            WHEN dados.cnae_2_secao = 'G' THEN 'Comércio'
            WHEN dados.cnae_2_secao IN ('H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U') THEN 'Serviços'
            WHEN dados.cnae_2_secao = 'O' THEN 'Administração Pública'
            WHEN dados.cnae_2_secao = 'A' THEN 'Agropecuária, extração vegetal, caça e pesca'
            ELSE 'Outro'
        END AS grupo,
        COUNT(*) AS num_estabelecimento
        FROM `basedosdados.br_me_caged.microdados_movimentacao` AS dados
        LEFT JOIN (SELECT DISTINCT id_municipio, nome FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
        ON dados.id_municipio = diretorio_id_municipio.id_municipio
        WHERE
        (dados.cnae_2_subclasse LIKE '05%' OR dados.cnae_2_subclasse LIKE '07%' OR dados.cnae_2_subclasse LIKE '08%' OR
        dados.cnae_2_secao = 'C' OR
        dados.cnae_2_subclasse LIKE '352%' OR dados.cnae_2_subclasse LIKE '351%' OR dados.cnae_2_subclasse LIKE '36%' OR dados.cnae_2_subclasse LIKE '37%' OR dados.cnae_2_subclasse LIKE '38%' OR
        dados.cnae_2_secao = 'F' OR
        dados.cnae_2_secao = 'G' OR
        dados.cnae_2_secao IN ('H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U') OR
        dados.cnae_2_secao = 'O' OR
        dados.cnae_2_secao = 'A')
        AND dados.ano = {ano} 
        AND dados.mes = {mes}
        GROUP BY
        dados.ano,
        dados.mes,
        dados.id_municipio,
        diretorio_id_municipio.nome,
        tamanho_estabelecimento_janeiro,
        CASE
            WHEN dados.cnae_2_subclasse LIKE '05%' OR dados.cnae_2_subclasse LIKE '07%' OR dados.cnae_2_subclasse LIKE '08%' THEN 'Extrativa mineral'
            WHEN dados.cnae_2_secao = 'C' THEN 'Indústria de transformação'
            WHEN dados.cnae_2_subclasse LIKE '352%' OR dados.cnae_2_subclasse LIKE '351%' OR dados.cnae_2_subclasse LIKE '36%' OR dados.cnae_2_subclasse LIKE '37%' OR dados.cnae_2_subclasse LIKE '38%' THEN 'Serviços industriais de utilidade pública'
            WHEN dados.cnae_2_secao = 'F' THEN 'Construção Civil'
            WHEN dados.cnae_2_secao = 'G' THEN 'Comércio'
            WHEN dados.cnae_2_secao IN ('H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U') THEN 'Serviços'
            WHEN dados.cnae_2_secao = 'O' THEN 'Administração Pública'
            WHEN dados.cnae_2_secao = 'A' THEN 'Agropecuária, extração vegetal, caça e pesca'
            ELSE 'Outro'
        END
        ORDER BY
        dados.ano,
        dados.mes,
        dados.id_municipio,
        grupo    
        """
    df = bd.read_sql(query, billing_project_id='fair-kingdom-372516')
    if df.shape[0]:
        df['tamanho_estabelecimento_janeiro'] = df['tamanho_estabelecimento_janeiro'].astype(int)

        dados = {
            'tamanho_estabelecimento_janeiro': [2, 3, 4, 5, 6, 7, 8, 9, 10],
            'Faixa de Vínculos': [
                'De 1 a 4 vínculos',
                'De 5 a 9 vínculos',
                'De 10 a 19 vínculos',
                'De 20 a 49 vínculos',
                'De 50 a 99 vínculos',
                'De 100 a 249 vínculos',
                'De 250 a 499 vínculos',
                'De 500 a 999 vínculos',
                '1000 ou mais vínculos'
            ]
        }

        # Criando o DataFrame
        tamanho = pd.DataFrame(dados)

        df = pd.merge(df, tamanho, on='tamanho_estabelecimento_janeiro', how='inner')

        df['tipologia'] = df['grupo'].astype(str) + ' - ' + df['Faixa de Vínculos'].astype(str)
        
        df = df[['id_municipio', 'tipologia', 'num_estabelecimento','ano', 'mes']]

        df['num_estabelecimento'] = df['num_estabelecimento'].astype(int)
        
        df = df.pivot_table(index=['id_municipio','ano','mes'],
                                columns='tipologia',
                                values='num_estabelecimento',
                                fill_value=0).reset_index()



        def get_colunas(nome):
            colunas = [
                f'{nome} - 1000 ou mais vínculos',
                f'{nome} - De 1 a 4 vínculos',
                f'{nome} - De 10 a 19 vínculos',
                f'{nome} - De 100 a 249 vínculos',
                f'{nome} - De 20 a 49 vínculos',
                f'{nome} - De 250 a 499 vínculos',
                f'{nome} - De 5 a 9 vínculos',
                f'{nome} - De 50 a 99 vínculos',
                f'{nome} - De 500 a 999 vínculos'
            ]
            return colunas

        df['Total - De 1 a 4']     = 0
        df['Total - De 5 a 9']     = 0
        df['Total - De 10 a 19']   = 0
        df['Total - De 20 a 49']   = 0
        df['Total - De 50 a 99']   = 0
        df['Total - De 100 a 249'] = 0
        df['Total - De 250 a 499'] = 0
        df['Total - De 500 a 999'] = 0
        df['Total - 1000 ou Mais'] = 0


        for coluna in ['Extrativa mineral', 'Indústria de transformação', 'Serviços industriais de utilidade pública', 'Construção Civil', 'Comércio', 'Serviços', 'Administração Pública', 'Agropecuária, extração vegetal, caça e pesca']:  
            df[f'{coluna} - Total'] = df[get_colunas(coluna)].sum(axis=1)
            df['Total - De 1 a 4'] = df['Total - De 1 a 4'] + df[f'{coluna} - De 1 a 4 vínculos']
            df['Total - De 5 a 9'] = df['Total - De 5 a 9'] + df[f'{coluna} - De 5 a 9 vínculos']
            df['Total - De 10 a 19'] = df['Total - De 10 a 19'] + df[f'{coluna} - De 10 a 19 vínculos']
            df['Total - De 20 a 49'] = df['Total - De 20 a 49'] + df[f'{coluna} - De 20 a 49 vínculos']
            df['Total - De 50 a 99'] = df['Total - De 50 a 99'] + df[f'{coluna} - De 50 a 99 vínculos']
            df['Total - De 250 a 499'] = df['Total - De 250 a 499'] + df[f'{coluna} - De 250 a 499 vínculos']
            df['Total - De 500 a 999'] = df['Total - De 500 a 999'] + df[f'{coluna} - De 500 a 999 vínculos']
            df['Total - 1000 ou Mais'] = df['Total - 1000 ou Mais'] + df[f'{coluna} - 1000 ou mais vínculos']

        df.columns = ['codmun','ano','mes','administracaopublica1000oumaisvinculos','administracaopublicade1a4vinculos','administracaopublicade10a19vinculos','administracaopublicade100a249vinculos','administracaopublicade20a49vinculos','administracaopublicade250a499vinculos','administracaopublicade5a9vinculos','administracaopublicade50a99vinculos','administracaopublicade500a999vinculos','agropecuariaextracavegetalcacaepesca1000oumaisvinculos','agropecuariaextracavegetalcacaepescade1a4vinculos','agropecuariaextracavegetalcacaepescade10a19vinculos','agropecuariaextracavegetalcacaepescade100a249vinculos','agropecuariaextracavegetalcacaepescade20a49vinculos','agropecuariaextracavegetalcacaepescade250a499vinculos','agropecuariaextracavegetalcacaepescade5a9vinculos','agropecuariaextracavegetalcacaepescade50a99vinculos','agropecuariaextracavegetalcacaepescade500a999vinculos','comercio1000oumaisvinculos','comerciode1a4vinculos','comerciode10a19vinculos','comerciode100a249vinculos','comerciode20a49vinculos','comerciode250a499vinculos','comerciode5a9vinculos','comerciode50a99vinculos','comerciode500a999vinculos','construcaocivil1000oumaisvinculos','construcaocivilde1a4vinculos','construcaocivilde10a19vinculos','construcaocivilde100a249vinculos','construcaocivilde20a49vinculos','construcaocivilde250a499vinculos','construcaocivilde5a9vinculos','construcaocivilde50a99vinculos','construcaocivilde500a999vinculos','extrativamineral1000oumaisvinculos','extrativamineralde1a4vinculos','extrativamineralde10a19vinculos','extrativamineralde100a249vinculos','extrativamineralde20a49vinculos','extrativamineralde250a499vinculos','extrativamineralde5a9vinculos','extrativamineralde50a99vinculos','extrativamineralde500a999vinculos','industriadetransformacao1000oumaisvinculos','industriadetransformacaode1a4vinculos','industriadetransformacaode10a19vinculos','industriadetransformacaode100a249vinculos','industriadetransformacaode20a49vinculos','industriadetransformacaode250a499vinculos','industriadetransformacaode5a9vinculos','industriadetransformacaode50a99vinculos','industriadetransformacaode500a999vinculos','servicos1000oumaisvinculos','servicosde1a4vinculos','servicosde10a19vinculos','servicosde100a249vinculos','servicosde20a49vinculos','servicosde250a499vinculos','servicosde5a9vinculos','servicosde50a99vinculos','servicosde500a999vinculos','servicosindustriaisdeutilidadepublica1000oumaisvinculos','servicosindustriaisdeutilidadepublicade1a4vinculos','servicosindustriaisdeutilidadepublicade10a19vinculos','servicosindustriaisdeutilidadepublicade100a249vinculos','servicosindustriaisdeutilidadepublicade20a49vinculos','servicosindustriaisdeutilidadepublicade250a499vinculos','servicosindustriaisdeutilidadepublicade5a9vinculos','servicosindustriaisdeutilidadepublicade50a99vinculos','servicosindustriaisdeutilidadepublicade500a999vinculos','totalde1a4','totalde5a9','totalde10a19','totalde20a49','totalde50a99','totalde100a249','totalde250a499','totalde500a999','total1000oumais','extrativamineraltotal','industriadetransformacaototal','servicosindustriaisdeutilidadepublicatotal','construcaociviltotal','comerciototal','servicostotal','administracaopublicatotal','agropecuariaextracavegetalcacaepescatotal']

        df = df [['codmun','administracaopublica1000oumaisvinculos','administracaopublicade1a4vinculos','administracaopublicade10a19vinculos','administracaopublicade100a249vinculos','administracaopublicade20a49vinculos','administracaopublicade250a499vinculos','administracaopublicade5a9vinculos','administracaopublicade50a99vinculos','administracaopublicade500a999vinculos','agropecuariaextracavegetalcacaepesca1000oumaisvinculos','agropecuariaextracavegetalcacaepescade1a4vinculos','agropecuariaextracavegetalcacaepescade10a19vinculos','agropecuariaextracavegetalcacaepescade100a249vinculos','agropecuariaextracavegetalcacaepescade20a49vinculos','agropecuariaextracavegetalcacaepescade250a499vinculos','agropecuariaextracavegetalcacaepescade5a9vinculos','agropecuariaextracavegetalcacaepescade50a99vinculos','agropecuariaextracavegetalcacaepescade500a999vinculos','comercio1000oumaisvinculos','comerciode1a4vinculos','comerciode10a19vinculos','comerciode100a249vinculos','comerciode20a49vinculos','comerciode250a499vinculos','comerciode5a9vinculos','comerciode50a99vinculos','comerciode500a999vinculos','construcaocivil1000oumaisvinculos','construcaocivilde1a4vinculos','construcaocivilde10a19vinculos','construcaocivilde100a249vinculos','construcaocivilde20a49vinculos','construcaocivilde250a499vinculos','construcaocivilde5a9vinculos','construcaocivilde50a99vinculos','construcaocivilde500a999vinculos','extrativamineral1000oumaisvinculos','extrativamineralde1a4vinculos','extrativamineralde10a19vinculos','extrativamineralde100a249vinculos','extrativamineralde20a49vinculos','extrativamineralde250a499vinculos','extrativamineralde5a9vinculos','extrativamineralde50a99vinculos','extrativamineralde500a999vinculos','industriadetransformacao1000oumaisvinculos','industriadetransformacaode1a4vinculos','industriadetransformacaode10a19vinculos','industriadetransformacaode100a249vinculos','industriadetransformacaode20a49vinculos','industriadetransformacaode250a499vinculos','industriadetransformacaode5a9vinculos','industriadetransformacaode50a99vinculos','industriadetransformacaode500a999vinculos','servicos1000oumaisvinculos','servicosde1a4vinculos','servicosde10a19vinculos','servicosde100a249vinculos','servicosde20a49vinculos','servicosde250a499vinculos','servicosde5a9vinculos','servicosde50a99vinculos','servicosde500a999vinculos','servicosindustriaisdeutilidadepublica1000oumaisvinculos','servicosindustriaisdeutilidadepublicade1a4vinculos','servicosindustriaisdeutilidadepublicade10a19vinculos','servicosindustriaisdeutilidadepublicade100a249vinculos','servicosindustriaisdeutilidadepublicade20a49vinculos','servicosindustriaisdeutilidadepublicade250a499vinculos','servicosindustriaisdeutilidadepublicade5a9vinculos','servicosindustriaisdeutilidadepublicade50a99vinculos','servicosindustriaisdeutilidadepublicade500a999vinculos','totalde1a4','totalde5a9','totalde10a19','totalde20a49','totalde50a99','totalde100a249','totalde250a499','totalde500a999','total1000oumais','extrativamineraltotal','industriadetransformacaototal','servicosindustriaisdeutilidadepublicatotal','construcaociviltotal','comerciototal','servicostotal','administracaopublicatotal','agropecuariaextracavegetalcacaepescatotal','mes','ano']]
        return df
        # df = pd.merge(df,mun,on='codmun', how='left')
    return np.array([])



def run_table_num_estabelecimento_setor_tamanho():
    try:
        mes, ano = get_ultimo_mes_ano(table_name)
        ultima_data = datetime(ano, mes, 1)
        ultima_data = ultima_data + relativedelta(months=1)
        data_atual = datetime.now()
        mun = get_municipio()
        while ultima_data < data_atual:
            ano = ultima_data.year
            mes = ultima_data.month
            df = dataframe(ano,mes)
            if df.shape[0]:
                df = pd.merge(df,mun,on='codmun', how='left')
                add_values(df,table_name)
                
            ultima_data = ultima_data + relativedelta(months=1)     
        
        
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")   
        