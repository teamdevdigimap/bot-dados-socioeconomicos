import basedosdados as bd
from datetime import datetime
import pandas as pd
from utils.utils import add_values, get_municipio, get_ultimo_ano
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()
table_name = 'table_obitos_por_causas_evitaveis_em_menores_de_5_anos'

def dataframe(ano):
    query =f"""
    WITH 
    dicionario_tipo_obito AS (
        SELECT
            chave AS chave_tipo_obito,
            valor AS descricao_tipo_obito
        FROM `basedosdados.br_ms_sim.dicionario`
        WHERE
            TRUE
            AND nome_coluna = 'tipo_obito'
            AND id_tabela = 'microdados'
    )
    SELECT
        ano,
        descricao_tipo_obito AS tipo_obito,
        dados.causa_basica AS causa_basica,
        diretorio_causa_basica.descricao_subcategoria AS causa_basica_descricao_subcategoria,
        diretorio_causa_basica.descricao_categoria AS causa_basica_descricao_categoria,
        diretorio_causa_basica.descricao_capitulo AS causa_basica_descricao_capitulo,
        data_obito,
        idade,
        dados.id_municipio_residencia AS id_municipio_residencia,
        diretorio_id_municipio_residencia.nome AS id_municipio_residencia_nome,
        dados.id_municipio_ocorrencia AS id_municipio_ocorrencia,
        diretorio_id_municipio_ocorrencia.nome AS id_municipio_ocorrencia_nome
    FROM `basedosdados.br_ms_sim.microdados` AS dados
    LEFT JOIN `dicionario_tipo_obito`
        ON dados.tipo_obito = chave_tipo_obito
    LEFT JOIN (SELECT DISTINCT subcategoria,descricao_subcategoria,descricao_categoria,descricao_capitulo  FROM `basedosdados.br_bd_diretorios_brasil.cid_10`) AS diretorio_causa_basica
        ON dados.causa_basica = diretorio_causa_basica.subcategoria
    LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio_residencia
        ON dados.id_municipio_residencia = diretorio_id_municipio_residencia.id_municipio
    LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio_ocorrencia
        ON dados.id_municipio_ocorrencia = diretorio_id_municipio_ocorrencia.id_municipio

        where ano = {ano} and idade <= 5
    """
    
    df = bd.read_sql(query, billing_project_id=os.environ['USER'])

    #causas evitaveis:
    reduzivel_pelas_acoes_de_imunizacao = ['A17', 'A19', 'A33', 'A35', 'A36', 'A37', 'A80', 'B05', 'B06', 'B16', 'B26', 'G000', 'P350', 'P353']

    #Reduzíveis por adequada atenção à mulher na gestação
    reduziveis_por_adequada_atenção_a_mulher_na_gestacao = ['B20', 'B21', 'B22', 'B23', 'B24', 'P022', 'P023', 'P027', 'P028', 'P029', 'P00', 'P01', 'P02', 'P03', 'P04', 'P01', 'P05', 'P07', 'P220', 'P26', 'P52', 'P550', 'P551', 'P558', 'P559', 'P56', 'P57', 'P77']

    #Reduzíveis por adequada atenção à mulher no parto
    reduziveis_por_adequada_atencao_a_mulher_no_parto = ['P020', 'P021', 'P024', 'P025', 'P026', 'P03', 'P08', 'P10', 'P11', 'P12', 'P13', 'P14', 'P15', 'P20', 'P21', 'P240', 'P241', 'P242', 'P248', 'P249']

    #Reduzíveis por adequada atenção ao recém-nascido
    reduziveis_por_adequada_atencao_ao_recemnascido = ['P221', 'P228', 'P229', 'P23', 'P25', 'P27', 'P28', 'P351', 'P352', 'P354', 'P359', 'P36', 'P39', 'P50', 'P51', 'P53', 'P54', 'P58', 'P59', 'P70', 'P71', 'P72', 'P73', 'P74', 'P60', 'P61', 'P75', 'P76', 'P78', 'P80', 'P81', 'P82', 'P83', 'P90', 'P91', 'P92', 'P93', 'P94', 'P960', 'P961', 'P962', 'P963', 'P964', 'P965', 'P966', 'P967', 'P968']

    #Reduzíveis por ações de diagnóstico e tratamento adequado 
    reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado = ['A15', 'A16', 'A18', 'G001', 'G002', 'G003', 'G004', 'G005', 'G006', 'G007', 'G008', 'G009', 'G03', 'J00', 'J01', 'J02', 'J03', 'J04', 'J05', 'J06', 'J12', 'J13', 'J14', 'J15', 'J16', 'J17', 'J18', 'J20', 'J21', 'J22', 'J384', 'J40', 'J41', 'J42', 'J45', 'J46', 'J47', 'J68', 'J69', 'A70', 'A71', 'A72', 'A73', 'A74', 'A30', 'A31', 'A32', 'A38', 'A39', 'A40', 'A41', 'A46', 'A49', 'E030', 'E031', 'E10', 'E11', 'E12', 'E13', 'E14', 'E700', 'E730', 'G40', 'G41', 'Q90', 'N390', 'I00', 'I01', 'I02', 'I03', 'I04', 'I05', 'I06', 'I07', 'I08', 'I09']

    #Reduzíveis por ações promoção à saúde vinculadas a ações de atenção
    reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao= ['A00', 'A01', 'A02', 'A03', 'A04', 'A05', 'A06', 'A07', 'A08', 'A09', 'A20', 'A21', 'A22', 'A23', 'A24', 'A25', 'A26', 'A27', 'A28', 'A90', 'A91', 'A92', 'A93', 'A94', 'A95', 'A96', 'A97', 'A98', 'A99', 'A75', 'A76', 'A77', 'A78', 'A79', 'A82', 'B50', 'B51', 'B52', 'B53', 'B54', 'B55', 'B56', 'B57', 'B58', 'B59', 'B60', 'B61', 'B62', 'B63', 'B64', 'B65', 'B66', 'B67', 'B68', 'B69', 'B70', 'B71', 'B72', 'B73', 'B74', 'B75', 'B76', 'B77', 'B78', 'B79', 'B80', 'B81', 'B82', 'B83', 'B99', 'D50', 'D51', 'D52', 'D53', 'E40', 'E41', 'E42', 'E43', 'E44', 'E45', 'E46', 'E47', 'E48', 'E49', 'E50', 'E51', 'E52', 'E53', 'E54', 'E55', 'E56', 'E57', 'E58', 'E59', 'E60', 'E61', 'E62', 'E63', 'E64', 'E86', 'V01', 'V02', 'V03', 'V04', 'V05', 'V06', 'V07', 'V08', 'V09', 'V10', 'V11', 'V12', 'V13', 'V14', 'V15', 'V16', 'V17', 'V18', 'V19', 'V20', 'V21', 'V22', 'V23', 'V24', 'V25', 'V26', 'V27', 'V28', 'V29', 'V30', 'V31', 'V32', 'V33', 'V34', 'V35', 'V36', 'V37', 'V38', 'V39', 'V40', 'V41', 'V42', 'V43', 'V44', 'V45', 'V46', 'V47', 'V48', 'V49', 'V50', 'V51', 'V52', 'V53', 'V54', 'V55', 'V56', 'V57', 'V58', 'V59', 'V60', 'V61', 'V62', 'V63', 'V64', 'V65', 'V66', 'V67', 'V68', 'V69', 'V70', 'V71', 'V72', 'V73', 'V74', 'V75', 'V76', 'V77', 'V78', 'V79', 'V80', 'V81', 'V82', 'V83', 'V84', 'V85', 'V86', 'V87', 'V88', 'V89', 'V90', 'V91', 'V92', 'V93', 'V94', 'V95', 'V96', 'V97', 'V98', 'V99', 'X40', 'X41', 'X42', 'X43', 'X44', 'X45', 'X46', 'X47', 'X48', 'X49', 'R95', 'W00', 'W01', 'W02', 'W03', 'W04', 'W05', 'W06', 'W07', 'W08', 'W09', 'W10', 'W11', 'W12', 'W13', 'W14', 'W15', 'W16', 'W17', 'W18', 'W19', 'X00', 'X01', 'X02', 'X03', 'X04', 'X05', 'X06', 'X07', 'X08', 'X09', 'X30', 'X31', 'X32', 'X33', 'X34', 'X35', 'X36', 'X37', 'X38', 'X39', 'W65', 'W66', 'W67', 'W68', 'W69', 'W70', 'W71', 'W72', 'W73', 'W74', 'W75', 'W76', 'W77', 'W78', 'W79', 'W80', 'W81', 'W82', 'W83', 'W84', 'W85', 'W86', 'W87', 'W88', 'W89', 'W90', 'W91', 'W92', 'W93', 'W94', 'W95', 'W96', 'W97', 'W98', 'W99', 'X85', 'Y00', 'Y01', 'Y02', 'Y03', 'Y04', 'Y05', 'Y06', 'Y07', 'Y08', 'Y09', 'Y10', 'Y11', 'Y12', 'Y13', 'Y14', 'Y15', 'Y16', 'Y17', 'Y18', 'Y19', 'Y20', 'Y21', 'Y22', 'Y23', 'Y24', 'Y25', 'Y26', 'Y27', 'Y28', 'Y29', 'Y30', 'Y31', 'Y32', 'Y33', 'Y34', 'W20', 'W21', 'W22', 'W23', 'W24', 'W25', 'W26', 'W27', 'W28', 'W29', 'W30', 'W31', 'W32', 'W33', 'W34', 'W35', 'W36', 'W37', 'W38', 'W39', 'W40', 'W41', 'W42', 'W43', 'W44', 'W45', 'W46', 'W47', 'W48', 'W49', 'Y60', 'Y61', 'Y62', 'Y63', 'Y64', 'Y65', 'Y66', 'Y67', 'Y68', 'Y69', 'Y83', 'Y84', 'Y40', 'Y41', 'Y42', 'Y43', 'Y44', 'Y45', 'Y46', 'Y47', 'Y48', 'Y49', 'Y50', 'Y51', 'Y52', 'Y53', 'Y54', 'Y55', 'Y56', 'Y57', 'Y58', 'Y59']

    #Causas mal definidas
    causas_mal_definidas = ['R00', 'R01', 'R02', 'R03', 'R04', 'R05', 'R06', 'R07', 'R08', 'R09', 'R10', 'R11', 'R12', 'R13', 'R14', 'R15', 'R16', 'R17', 'R18', 'R19', 'R20', 'R21', 'R22', 'R23', 'R24', 'R25', 'R26', 'R27', 'R28', 'R29', 'R30', 'R31', 'R32', 'R33', 'R34', 'R35', 'R36', 'R37', 'R38', 'R39', 'R40', 'R41', 'R42', 'R43', 'R44', 'R45', 'R46', 'R47', 'R48', 'R49', 'R50', 'R51', 'R52', 'R53', 'R54', 'R55', 'R56', 'R57', 'R58', 'R59', 'R60', 'R61', 'R62', 'R63', 'R64', 'R65', 'R66', 'R67', 'R68', 'R69', 'R70', 'R71', 'R72', 'R73', 'R74', 'R75', 'R76', 'R77', 'R78', 'R79', 'R80', 'R81', 'R82', 'R83', 'R84', 'R85', 'R86', 'R87', 'R88', 'R89', 'R90', 'R91', 'R92', 'R93', 'R96', 'R97', 'R98', 'R99', 'P95', 'P969']

    nao_claramente_evitaveis = reduzivel_pelas_acoes_de_imunizacao + reduziveis_por_adequada_atenção_a_mulher_na_gestacao + reduziveis_por_adequada_atencao_a_mulher_no_parto + reduziveis_por_adequada_atencao_ao_recemnascido + reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado + reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao + causas_mal_definidas


    reduzivel_pelas_acoes_de_imunizacao                                 = df[df['causa_basica'].str.startswith(tuple(reduzivel_pelas_acoes_de_imunizacao))]
    reduziveis_por_adequada_atenção_a_mulher_na_gestacao                = df[df['causa_basica'].str.startswith(tuple(reduziveis_por_adequada_atenção_a_mulher_na_gestacao))]
    reduziveis_por_adequada_atencao_a_mulher_no_parto                   = df[df['causa_basica'].str.startswith(tuple(reduziveis_por_adequada_atencao_a_mulher_no_parto))]
    reduziveis_por_adequada_atencao_ao_recemnascido                     = df[df['causa_basica'].str.startswith(tuple(reduziveis_por_adequada_atencao_ao_recemnascido))]
    reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado           = df[df['causa_basica'].str.startswith(tuple(reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado))]
    reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao = df[df['causa_basica'].str.startswith(tuple(reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao))]
    causas_mal_definidas                                                = df[df['causa_basica'].str.startswith(tuple(causas_mal_definidas))]
    nao_claramente_evitaveis                                            = df[~df['causa_basica'].str.startswith(tuple(nao_claramente_evitaveis))] #As causas não listadas anteriormente


    ocorrencia_reduzivel_pelas_acoes_de_imunizacao = reduzivel_pelas_acoes_de_imunizacao.groupby('id_municipio_ocorrencia').size().reset_index(name='Ocorrência reduzível pelas ações de imunização')
    ocorrencia_reduzivel_pelas_acoes_de_imunizacao = ocorrencia_reduzivel_pelas_acoes_de_imunizacao.rename(columns={'id_municipio_ocorrencia':'codmun'})
    ocorrencia_reduzivel_pelas_acoes_de_imunizacao['codmun'] = ocorrencia_reduzivel_pelas_acoes_de_imunizacao['codmun'].astype(int)
    reduzivel_pelas_acoes_de_imunizacao = ocorrencia_reduzivel_pelas_acoes_de_imunizacao
    reduzivel_pelas_acoes_de_imunizacao.fillna(0, inplace=True)


    ocorrencia_reduziveis_por_adequada_atenção_a_mulher_na_gestacao = reduziveis_por_adequada_atenção_a_mulher_na_gestacao.groupby('id_municipio_ocorrencia').size().reset_index(name='Ocorrências reduzíveis por adequada atenção à mulher na gestação.')
    ocorrencia_reduziveis_por_adequada_atenção_a_mulher_na_gestacao = ocorrencia_reduziveis_por_adequada_atenção_a_mulher_na_gestacao.rename(columns={'id_municipio_ocorrencia':'codmun'})
    ocorrencia_reduziveis_por_adequada_atenção_a_mulher_na_gestacao['codmun'] = ocorrencia_reduziveis_por_adequada_atenção_a_mulher_na_gestacao['codmun'].astype(int)
    reduziveis_por_adequada_atenção_a_mulher_na_gestacao = ocorrencia_reduziveis_por_adequada_atenção_a_mulher_na_gestacao
    reduziveis_por_adequada_atenção_a_mulher_na_gestacao.fillna(0, inplace=True)


    ocorrencia_reduziveis_por_adequada_atencao_a_mulher_no_parto = reduziveis_por_adequada_atencao_a_mulher_no_parto.groupby('id_municipio_ocorrencia').size().reset_index(name='Ocorrências reduzíveis por adequada atenção à mulher no parto.')
    ocorrencia_reduziveis_por_adequada_atencao_a_mulher_no_parto = ocorrencia_reduziveis_por_adequada_atencao_a_mulher_no_parto.rename(columns={'id_municipio_ocorrencia':'codmun'})
    ocorrencia_reduziveis_por_adequada_atencao_a_mulher_no_parto['codmun'] = ocorrencia_reduziveis_por_adequada_atencao_a_mulher_no_parto['codmun'].astype(int)
    reduziveis_por_adequada_atencao_a_mulher_no_parto = ocorrencia_reduziveis_por_adequada_atencao_a_mulher_no_parto
    reduziveis_por_adequada_atencao_a_mulher_no_parto.fillna(0, inplace=True)


    ocorrencia_reduziveis_por_adequada_atencao_ao_recemnascido = reduziveis_por_adequada_atencao_ao_recemnascido.groupby('id_municipio_ocorrencia').size().reset_index(name='Ocorrências reduzíveis por adequada atenção ao recém-nascido.')
    ocorrencia_reduziveis_por_adequada_atencao_ao_recemnascido = ocorrencia_reduziveis_por_adequada_atencao_ao_recemnascido.rename(columns={'id_municipio_ocorrencia':'codmun'})
    ocorrencia_reduziveis_por_adequada_atencao_ao_recemnascido['codmun'] = ocorrencia_reduziveis_por_adequada_atencao_ao_recemnascido['codmun'].astype(int)
    reduziveis_por_adequada_atencao_ao_recemnascido = ocorrencia_reduziveis_por_adequada_atencao_ao_recemnascido
    reduziveis_por_adequada_atencao_ao_recemnascido.fillna(0, inplace=True)


    ocorrencia_reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado = reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado.groupby('id_municipio_ocorrencia').size().reset_index(name='Ocorrências reduzíveis por ações de diagnóstico e tratamento adequado.')
    ocorrencia_reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado = ocorrencia_reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado.rename(columns={'id_municipio_ocorrencia':'codmun'})
    ocorrencia_reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado['codmun'] = ocorrencia_reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado['codmun'].astype(int)
    reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado = ocorrencia_reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado
    reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado.fillna(0, inplace=True)


    ocorrencia_reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao = reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao.groupby('id_municipio_ocorrencia').size().reset_index(name='Ocorrências reduzíveis por ações de promoção à saúde vinculadas a ações de atenção.')
    ocorrencia_reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao = ocorrencia_reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao.rename(columns={'id_municipio_ocorrencia':'codmun'})
    ocorrencia_reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao['codmun'] = ocorrencia_reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao['codmun'].astype(int)
    reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao = ocorrencia_reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao
    reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao.fillna(0, inplace=True)


    ocorrencia_causas_mal_definidas = causas_mal_definidas.groupby('id_municipio_ocorrencia').size().reset_index(name='Ocorrência de causas mal definidas.')
    ocorrencia_causas_mal_definidas = ocorrencia_causas_mal_definidas.rename(columns={'id_municipio_ocorrencia':'codmun'})
    ocorrencia_causas_mal_definidas['codmun'] = ocorrencia_causas_mal_definidas['codmun'].astype(int)
    causas_mal_definidas = ocorrencia_causas_mal_definidas
    causas_mal_definidas.fillna(0, inplace=True)


    ocorrencia_nao_claramente_evitaveis = nao_claramente_evitaveis.groupby('id_municipio_ocorrencia').size().reset_index(name='Ocorrências não claramente evitáveis.')
    ocorrencia_nao_claramente_evitaveis = ocorrencia_nao_claramente_evitaveis.rename(columns={'id_municipio_ocorrencia':'codmun'})
    ocorrencia_nao_claramente_evitaveis['codmun'] = ocorrencia_nao_claramente_evitaveis['codmun'].astype(int)
    nao_claramente_evitaveis = ocorrencia_nao_claramente_evitaveis
    nao_claramente_evitaveis.fillna(0, inplace=True)


    tudo_junto = pd.merge(reduzivel_pelas_acoes_de_imunizacao,reduziveis_por_adequada_atenção_a_mulher_na_gestacao,on='codmun', how='outer')
    tudo_junto = pd.merge(tudo_junto,reduziveis_por_adequada_atencao_a_mulher_no_parto,on='codmun', how='outer')
    tudo_junto = pd.merge(tudo_junto,reduziveis_por_adequada_atencao_ao_recemnascido,on='codmun', how='outer')
    tudo_junto = pd.merge(tudo_junto,reduziveis_por_acoes_de_diagnostico_e_tratamento_adequado,on='codmun', how='outer')
    tudo_junto = pd.merge(tudo_junto,reduziveis_por_acoes_promocao_a_saude_vinculadas_a_acoes_de_atencao,on='codmun', how='outer')
    tudo_junto = pd.merge(tudo_junto,causas_mal_definidas,on='codmun', how='outer')
    tudo_junto = pd.merge(tudo_junto,nao_claramente_evitaveis,on='codmun', how='outer')
    tudo_junto.fillna(0, inplace=True)
    tudo_junto['total_causas_evitaveis'] = tudo_junto.iloc[:, 1:].sum(axis=1) 
    tudo_junto.iloc[:, 1:] = tudo_junto.iloc[:, 1:].astype(int)
    tudo_junto['ano'] = ano
    tudo_junto.columns = tudo_junto.columns.str.replace('_', '', regex=False)
    tudo_junto = tudo_junto[['codmun', 'totalcausasevitaveis','ano']]
    tudo_junto['codmun'] = tudo_junto['codmun'].astype(str)
    
    if tudo_junto.shape[0]:
        return tudo_junto
    
    return np.array([])

def run_table_obitos_por_causas_evitaveis_em_menores_de_5_anos():
    try:
        mun  = get_municipio()
        ultimo_ano = get_ultimo_ano(table_name)
        ano_atual = datetime.now().year
        for ano in range(ultimo_ano+1, ano_atual+1):
            df = dataframe(ano)
            if df.shape[0]:
                df = pd.merge(df, mun, on='codmun', how='left') 
                add_values(df, table_name)
    except Exception as e:
        print(f"Erro ao atualizar da tabela {table_name} erro:\n{e}")  
