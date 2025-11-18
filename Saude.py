from tables.Saude.table_despesas_com_saude import table_despesas_com_saude
from tables.Saude.table_total_de_medicos_por_municipio import table_total_de_medicos_por_municipio
from tables.Saude.table_equipamentos_sus_por_tipos import table_equipamentos_sus_por_tipos
from tables.Saude.table_nascimentos import table_nascimentos
from tables.Saude.table_saude_obitos_prematuros import table_saude_obitos_prematuros
from tables.Saude.table_obitos import table_obitos
from tables.Saude.table_saude_internacoes_hospitalares import table_saude_internacoes_hospitalares
from tables.Saude.table_taxa_de_producao_ambulatorial import table_taxa_de_producao_ambulatorial
from tables.Saude.table_saude_prod_ambula_qtd_aprovada import table_saude_prod_ambula_qtd_aprovada
from tables.Saude.table_obitos_por_causas_evitaveis_em_menores_de_5_anos import table_obitos_por_causas_evitaveis_em_menores_de_5_anos
from tables.Saude.table_saude_leitos_internacao import table_saude_leitos_internacao
from tables.Saude.taxa_de_leitos_de_internecao import taxa_de_leitos_de_internecao

def saude_run():
    
    try:
        table_despesas_com_saude.run_table_despesas_com_saude()
    except Exception as e:
        print(f"Erro na tabela table_despesas_com_saude \n{e}")  
        
    try:
        table_total_de_medicos_por_municipio.run_table_total_de_medicos_por_municipio()
    except Exception as e:
        print(f"Erro na tabela table_total_de_medicos_por_municipio \n{e}")  
        
    try:
        table_equipamentos_sus_por_tipos.run_table_equipamentos_sus_por_tipos()
    except Exception as e:
        print(f"Erro na tabela table_equipamentos_sus_por_tipos \n{e}")  
        
    try:
        table_nascimentos.run_table_nascimentos()
    except Exception as e:
        print(f"Erro na tabela table_nascimentos \n{e}") 
        
    try:
        table_saude_obitos_prematuros.run_table_saude_obitos_prematuros()
    except Exception as e:
        print(f"Erro na tabela table_saude_obitos_prematuros \n{e}")
        
    try:
        table_obitos.run_table_obitos()
    except Exception as e:
        print(f"Erro na tabela table_obitos \n{e}")
        
    try:
        table_saude_internacoes_hospitalares.run_table_saude_internacoes_hospitalares()
    except Exception as e:
        print(f"Erro na tabela table_saude_internacoes_hospitalares \n{e}")   
        
        
    try:
        table_taxa_de_producao_ambulatorial.run_table_taxa_de_producao_ambulatorial()
    except Exception as e:
        print(f"Erro na tabela table_taxa_de_producao_ambulatorial \n{e}")      
        
        
    try:
        table_saude_prod_ambula_qtd_aprovada.run_table_saude_prod_ambula_qtd_aprovada()
    except Exception as e:
        print(f"Erro na tabela table_saude_prod_ambula_qtd_aprovada \n{e}")          
    
    try:
        table_obitos_por_causas_evitaveis_em_menores_de_5_anos.run_table_obitos_por_causas_evitaveis_em_menores_de_5_anos()
    except Exception as e:
        print(f"Erro na tabela table_obitos_por_causas_evitaveis_em_menores_de_5_anos \n{e}") 
        
        
    try:
        table_saude_leitos_internacao.run_table_saude_leitos_internacao()
    except Exception as e:
        print(f"Erro na tabela table_saude_leitos_internacao \n{e}") 
        

    try:
        taxa_de_leitos_de_internecao.run_taxa_de_leitos_de_internecao()
    except Exception as e:
        print(f"Erro na tabela taxa_de_leitos_de_internecao \n{e}")    