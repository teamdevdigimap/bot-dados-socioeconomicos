from tables.Financa.table_receitas_orcamentarias import table_receitas_orcamentarias
from tables.Financa.table_despesas_orcamentarias import table_despesas_orcamentarias
from tables.Financa.table_despesas_funcao import table_despesas_funcao
from tables.Financa.table_execucao_de_restos_a_pagar import table_execucao_de_restos_a_pagar
from tables.Financa.table_receita_corrente_liquida import table_receita_corrente_liquida
from tables.Financa.table_capacidade_de_pagamento_capag_municipios import table_capacidade_de_pagamento_capag_municipios
from tables.Financa.table_ifgf import table_ifgf
from tables.Financa.table_arrecadacao_cefem import table_arrecadacao_cefem
from tables.Financa.table_fundo_de_participacao_dos_municipios import table_fundo_de_participacao_dos_municipios
from utils.utils import update_data_ultima_coleta


rastreabilidade = 'rastreabilidade_financa'
##### FINANCA #####
def financa_run():

    try:
        table_receitas_orcamentarias.run_table_receitas_orcamentarias()
        pass
    except Exception as e:
        print(f"Erro na tabela table_receitas_orcamentarias \n{e}")  

    try:
        table_despesas_orcamentarias.run_table_despesas_orcamentarias()
        pass
    except Exception as e:
        print(f"Erro na tabela table_despesas_orcamentarias \n{e}")

    try:
        table_despesas_funcao.run_table_despesas_funcao()
        pass
    except Exception as e:
        print(f"Erro na tabela table_despesas_funcao \n{e}")

    try:
        # table_execucao_de_restos_a_pagar.run_table_execucao_de_restos_a_pagar()
        pass
    except Exception as e:
        print(f"Erro na tabela table_execucao_de_restos_a_pagar \n{e}")

    try:
        table_receita_corrente_liquida.run_table_receita_corrente_liquida()
        pass
    except Exception as e:
        print(f"Erro na tabela table_receita_corrente_liquida \n{e}")

    try:
        table_capacidade_de_pagamento_capag_municipios.run_table_capacidade_de_pagamento_capag_municipios()
        pass
    except Exception as e:
        print(f"Erro na tabela table_capacidade_de_pagamento_capag_municipios \n{e}")


    try:
        table_ifgf.run_table_ifgf()
        pass
    except Exception as e:
        print(f"Erro na tabela table_ifgf \n{e}")

    try:
        table_arrecadacao_cefem.run_table_arrecadacao_cefem()
        #update_data_ultima_coleta(rastreabilidade, 'table_arrecadacao_cefem')
        pass
    except Exception as e:
        print(f"Erro na tabela table_arrecadacao_cefem \n{e}")

    try:
        table_fundo_de_participacao_dos_municipios.run_table_fundo_de_participacao_dos_municipios()
        pass
    except Exception as e:
        print(f"Erro na tabela table_fundo_de_participacao_dos_municipios \n{e}")