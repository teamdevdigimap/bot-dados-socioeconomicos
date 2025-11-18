from tables.Emprego.table_num_admitidos_desligados import table_num_admitidos_desligados 
from tables.Emprego.table_num_vinculos_setor_tamanho import table_num_vinculos_setor_tamanho 
from tables.Emprego.table_massa_salarial_setor_tamanho import table_massa_salarial_setor_tamanho
from tables.Emprego.table_distribuicao_dos_empregados_formais_por_escolaridade_sexo import table_distribuicao_dos_empregados_formais_por_escolaridade_sexo
from tables.Emprego.table_num_estabelecimento_setor_tamanho import table_num_estabelecimento_setor_tamanho
from tables.Emprego.table_pib_municipal_e_desagregacoes import table_pib_municipal_e_desagregacoes
from tables.Emprego.table_pib_per_capita_municipal import table_pib_per_capita_municipal
from tables.Emprego.table_salario_medio import table_salario_medio

def emprego_run():
    
    try:
        table_num_admitidos_desligados.run_table_num_admitidos_desligados()
    except Exception as e:
        print(f"Erro na tabela table_num_admitidos_desligados \n{e}")  
        
        
    try:
        table_num_vinculos_setor_tamanho.run_table_num_vinculos_setor_tamanho()
    except Exception as e:
        print(f"Erro na tabela table_num_vinculos_setor_tamanho \n{e}")   
        
        
    try:
        table_massa_salarial_setor_tamanho.run_table_massa_salarial_setor_tamanho()
    except Exception as e:
        print(f"Erro na tabela table_massa_salarial_setor_tamanho \n{e}")           
        
        
    try:
        table_num_estabelecimento_setor_tamanho.run_table_num_estabelecimento_setor_tamanho()
    except Exception as e:
        print(f"Erro na tabela table_num_estabelecimento_setor_tamanho \n{e}")      
        
                 
    try:
        table_pib_municipal_e_desagregacoes.run_table_pib_municipal_e_desagregacoes()
    except Exception as e:
        print(f"Erro na tabela table_pib_municipal_e_desagregacoes \n{e}")      
        
    try:
        table_pib_per_capita_municipal.run_table_pib_per_capita_municipal()
        
    except Exception as e:
        print(f"Erro na tabela table_pib_per_capita_municipal \n{e}")     
        
    try:
        table_salario_medio.run_table_salario_medio()
    except Exception as e:
        print(f"Erro na tabela table_salario_medio \n{e}")           