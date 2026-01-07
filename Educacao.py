from tables.Educacao.table_estabelecimentos_educacao_basica import table_estabelecimentos_educacao_basica
from tables.Educacao.table_taxas_de_aprovacao import table_taxas_de_aprovacao
from tables.Educacao.table_nota_saeb_ensino_medio import table_nota_saeb_ensino_medio
from tables.Educacao.table_nota_saeb_anos_finais_fundamental import table_nota_saeb_anos_finais_fundamental
from tables.Educacao.table_nota_saeb_anos_iniciais_fundamental import table_nota_saeb_anos_iniciais_fundamental
from tables.Educacao.table_taxa_de_aprovacao_ideb_ensino_medio import table_taxa_de_aprovacao_ideb_ensino_medio
from tables.Educacao.table_taxa_de_aprovacao_ideb_anos_iniciais_fundamental import table_taxa_de_aprovacao_ideb_anos_iniciais_fundamental
from tables.Educacao.table_taxa_de_aprovacao_ideb_anos_finais_fundamental import table_taxa_de_aprovacao_ideb_anos_finais_fundamental
from tables.Educacao.table_percentual_de_docentes_em_exercicio_i import table_percentual_de_docentes_em_exercicio_i
from tables.Educacao.table_media_alunos_turma import table_media_alunos_turma
from tables.Educacao.table_taxa_de_distorcao_idade_serie import table_taxa_de_distorcao_idade_serie
from tables.Educacao.table_fluxo_da_educacao_superior import table_fluxo_da_educacao_superior
from tables.Educacao.table_taxas_de_abandono import table_taxas_de_abandono
from tables.Educacao.table_inse import table_inse
from tables.Educacao.table_taxas_de_reprovacao import table_taxas_de_reprovacao
from tables.Educacao.table_indicador_crianca_alfabetizada.table_indicador_crianca_alfabetizada import run_indicador_crianca_alfabetizada

def educacao_run():
  
    try:
        print("\nIniciando table_estabelecimentos_educacao_basica...\n")
        table_estabelecimentos_educacao_basica.run_table_estabelecimentos_educacao_basica()
        print("\nFinalizando table_estabelecimentos_educacao_basica...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_estabelecimentos_educacao_basica \n{e}")  

    try:
        print("\nIniciando table_taxas_de_aprovacao...\n")
        table_taxas_de_aprovacao.run_table_taxas_de_aprovacao()
        print("\nFinalizando table_taxas_de_aprovacao...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_taxas_de_aprovacao \n{e}")

    try:
        print("\nIniciando table_nota_saeb_ensino_medio...\n")
        table_nota_saeb_ensino_medio.run_table_nota_saeb_ensino_medio()
        print("\nFinalizando table_nota_saeb_ensino_medio...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_nota_saeb_ensino_medio \n{e}")  

    try:
        print("\nIniciando table_nota_saeb_anos_finais_fundamental...\n")
        table_nota_saeb_anos_finais_fundamental.run_table_nota_saeb_anos_finais_fundamental()
        print("\nFinalizando table_nota_saeb_anos_finais_fundamental...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_nota_saeb_anos_finais_fundamental \n{e}")

    try:
        print("\nIniciando table_nota_saeb_anos_iniciais_fundamental...\n")
        table_nota_saeb_anos_iniciais_fundamental.run_table_nota_saeb_anos_iniciais_fundamental()
        print("\nFinalizando table_nota_saeb_anos_iniciais_fundamental...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_nota_saeb_anos_finais_fundamental \n{e}")

    try:
        print("\nIniciando table_taxa_de_aprovacao_ideb_ensino_medio...\n")
        table_taxa_de_aprovacao_ideb_ensino_medio.run_ttable_taxa_de_aprovacao_ideb_ensino_medio()
        print("\nFinalizando table_taxa_de_aprovacao_ideb_ensino_medio...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_taxa_de_aprovacao_ideb_ensino_medio \n{e}")

    try:
        print("\nIniciando table_taxa_de_aprovacao_ideb_anos_iniciais_fundamental...\n")
        table_taxa_de_aprovacao_ideb_anos_iniciais_fundamental.run_table_taxa_de_aprovacao_ideb_anos_iniciais_fundamental()
        print("\nFinalizando table_taxa_de_aprovacao_ideb_anos_iniciais_fundamental...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_taxa_de_aprovacao_ideb_anos_iniciais_fundamental \n{e}")

    try:
        print("\nIniciando table_taxa_de_aprovacao_ideb_anos_finais_fundamental...\n")
        table_taxa_de_aprovacao_ideb_anos_finais_fundamental.run_table_taxa_de_aprovacao_ideb_anos_finais_fundamental()
        print("\nFinalizando table_taxa_de_aprovacao_ideb_anos_finais_fundamental...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_taxa_de_aprovacao_ideb_anos_finais_fundamental \n{e}")

    try:
        #table_percentual_de_docentes_em_exercicio_i.run_table_percentual_de_docentes_em_exercicio_i()
        pass
    except Exception as e:
        print(f"Erro na tabela table_percentual_de_docentes_em_exercicio_i \n{e}")

    try:
        #table_media_alunos_turma.run_table_media_alunos_turma()
        pass
    except Exception as e:
        print(f"Erro na tabela table_media_alunos_turma \n{e}")

    try:
        #table_taxa_de_distorcao_idade_serie.run_table_taxa_de_distorcao_idade_serie()
        pass
    except Exception as e:
        print(f"Erro na tabela table_taxa_de_distorcao_idade_serie \n{e}")

    try:
        #table_fluxo_da_educacao_superior.run_table_fluxo_da_educacao_superior()
        pass
    except Exception as e:
        print(f"Erro na tabela table_fluxo_da_educacao_superior \n{e}")

    try:
        #table_taxas_de_abandono.run_table_taxas_de_abandono()
        pass
    except Exception as e:
        print(f"Erro na tabela table_taxas_de_abandono \n{e}")

    try:
        #table_inse.run_table_inse()
        pass
    except Exception as e:
        print(f"Erro na tabela table_inse \n{e}")

    try:
        print("\nIniciando table_taxas_de_reprovacao...\n")
        table_taxas_de_reprovacao.run_table_taxas_de_reprovacao()
        print("\nFinalizando table_taxas_de_reprovacao...\n")
        pass
    except Exception as e:
        print(f"Erro na tabela table_taxas_de_reprovacao \n{e}")

    # try: 
    #     print("\nIniciando run_indicador_crianca_alfabetizada...\n")
    #     # Substitua pelo caminho do seu arquivo
    #     # arquivo_excel = "C:/Users/matheus.souza/Downloads/resultados_e_metas_municipios_2024.xlsx"
    #     arquivo_excel = None 
    #     run_indicador_crianca_alfabetizada(arquivo_excel)
    #     print("\nFinalizando run_indicador_crianca_alfabetizada...\n")
    #     pass
    # except Exception as e:
    #     print(f"Erro na tabela run_indicador_crianca_alfabetizada \n{e}")
    



    
