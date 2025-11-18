from datetime import datetime
import json
import pandas as pd
import requests
from datetime import datetime
from utils.utils import add_values, get_ultimo_ano, get_municipio

table_name = 'table_populacao_residente_por_cor_ou_raca'

def dataframe():
    #print(f">>>>>>>>>>>>>>>>>> tabela {table_name}")
    ultimo_ano = get_ultimo_ano(table_name)
    ano_atual = datetime.now().year
    df_final = pd.DataFrame()
    
    anos_processados = []
    for ano in range(ultimo_ano+1, ano_atual + 1):
        try:
            print(f'Ano - {ano}')
            url = f'https://servicodados.ibge.gov.br/api/v3/agregados/9606/periodos/{ano}/variaveis/93?localidades=N6[all]&classificacao=86[2776,2777,2778,2779,2780,95251]|287[100362]'
            
            response = requests.get(url)
            if response.status_code != 200:
                print(f"Erro na requisição para o ano {ano}: {response.status_code}")
                continue
            
            retorno = json.loads(response.text)
            # print(f">>>>>>>>>>>>> retorno para {ano}: {retorno}")
            
            if retorno:
                dados = []
                
                for variavel in retorno[0]['resultados']:
                    categoria = list(variavel['classificacoes'][0]['categoria'].values())[0]
                    if categoria == 'Total':
                        continue  # Ignora a categoria "Total" para manter consistência com o banco
                    for resultado in variavel['series']:
                        codmun = resultado['localidade']['id']
                        valor = resultado['serie'].get(f'{ano}', 0)
                        if isinstance(valor, str) and valor.replace('.', '').isdigit():
                            valor = int(float(valor))
                        elif not isinstance(valor, (int, float)):
                            valor = 0
                        dados.append({'codmun': codmun, 'ano': ano, 'corouraca': categoria, 'populacao': valor})
                
                df = pd.DataFrame(dados)
                df_municipios = get_municipio()
                df.replace('-', 0, inplace=True)
                df.fillna(0, inplace=True)
                
                df = df.merge(df_municipios, on='codmun', how='left')
                
                df_final = pd.concat([df_final, df], ignore_index=True)
                anos_processados.append(ano)
        except Exception as e:
            print(f"Erro ao processar ano {ano}: {e}")
    
    if not df_final.empty:
        df_final = df_final[['codmun', 'corouraca', 'populacao', 'ano', 'nome_sigla']]
        # print("Anos processados:", anos_processados)
        print(df_final)
        # print("Anos no DataFrame final:", df_final['ano'].unique())  # Verifica os anos no DataFrame final
        add_values(df_final, table_name)

def run_table_populacao_residente_por_cor_ou_raca():
    try:
        #print(f">>>>>>>>>>>>>>> dataframe gerando")
        dataframe()
    except Exception as e:
        raise Exception(f"Erro ao atualizar tabela {table_name}: {e}")
