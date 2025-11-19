from sqlalchemy import create_engine, Table, MetaData, update
import psycopg2
import pandas as pd
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import re
from psycopg2.extras import execute_batch

conn_params = {
        'dbname': 'sce',
        'user': 'azussocsql_ds_usr_vale',
        'password': '1jm0v3c45VDstkuVVAPd',
        'host': 'azussocsql02.postgres.database.azure.com',
        'port': '5432'
}

def add_values(df, table_name):
    # Adiciona valores no banco
    engine = None  # Inicializa a variável engine fora do try-except para acessá-la no finally
    try:
        # Cria a URL de conexão
        url = f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}"

        # Cria o engine de conexão
        engine = create_engine(url)

        # Adiciona o DataFrame à tabela no banco de dados
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Tabela {table_name} foi atualizada")

    except Exception as e:
        print(f"Ocorreu um erro ao atualizar a tabela {table_name}: {e}")
    
    finally:
        # Garantir que a conexão seja finalizada
        if engine:
            engine.dispose()  # Fecha a conexão com o banco
        print("Processo de atualização finalizado.")
          
def get_municipio_codmun6():
    # Conectando ao banco de dados
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Consulta para obter as colunas
        query = f"""
        select codmun, codmun6, nome_sigla from municipios_2022;
        """
        
        # Executando a consulta
        cur.execute(query)
        
        resultados = cur.fetchall()
        # Criando o DataFrame com os dados
        df = pd.DataFrame(resultados, columns=['codmun', 'codmun6','nome_sigla'])
        return df
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            cur.close()
            conn.close()         

def get_ultimo_ano(table_name):
    # Conectando ao banco de dados
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Consulta para obter as colunas
        query = f"""
        select ano from {table_name} order by ano desc limit 1;
        """
        
        # Executando a consulta
        cur.execute(query)
        
        # Obtendo os resultados
        ano = cur.fetchall()[0][0]
        return int(ano)
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            cur.close()
            conn.close()

def add_values(df,table_name):
    # Adiciona valores no banco
    
    # Cria a URL de conexão
    url = f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}"

    # Cria o engine de conexão
    engine = create_engine(url)
    # Adiciona o DataFrame à tabela no banco de dados
    df.to_sql(table_name, engine, if_exists='append', index=False)
    engine.dispose()
    print(f"Tabela {table_name} foi atualizado")

def get_ultimo_mes_ano(table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Consulta para obter as colunas
        query = f"""
        select mes, ano from {table_name} order by ano desc, mes desc limit 1;
        """
        
        # Executando a consulta
        cur.execute(query)
        
        # Obtendo os resultados
        mes, ano = cur.fetchall()[0]
        return mes,ano
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            cur.close()
            conn.close()

def get_municipio():
    # Conectando ao banco de dados
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Consulta para obter as colunas
        query = f"""
        select codmun, nome_sigla from municipios_2022;
        """
        
        # Executando a consulta
        cur.execute(query)
        
        resultados = cur.fetchall()
        # Criando o DataFrame com os dados
        df = pd.DataFrame(resultados, columns=['codmun', 'nome_sigla'])
        return df
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            cur.close()
            conn.close()
            
            
def get_table_massa_salarial_setor_tamanho(ano,table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        
        query = f"""
        select * from {table_name} where ano = {ano};
        """
        
        df = pd.read_sql_query(query, conn)
        
        if df.shape[0]:
            df = df.drop(columns=['createdat','updatedat'])

        return df
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            conn.close()            
            
            
def update_table_massa_salarial_setor_tamanho(df,table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                # Create a tuple of values for the SQL execution
                params = (row['total'], row['media'],row['mes'] ,row['codmun'], row['setor'], row['tamanhoestabelecimento'], row['ano'])

                # Check if params is correctly structured as a tuple/list
                sql = f"""
                UPDATE {table_name}
                SET total = %s, media = %s, updatedat = CURRENT_TIMESTAMP - INTERVAL '3 hours', mes = %s
                WHERE   codmun = %s
                    AND setor = %s
                    AND tamanhoestabelecimento = %s
                    AND ano = %s;
                """

                # Execute the batch update using execute_batch
                execute_batch(cur, sql, [params])  # Pass a list of one tuple

            # Commit the transaction after executing all updates
            conn.commit()
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()            
            
            
def get_equipamentos_sus_por_tipos(ano,table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        
        query = f"""
        select * from {table_name} where ano = {ano};
        """
        
        df = pd.read_sql_query(query, conn)
        
        if df.shape[0]:
            df = df.drop(columns=['createdat', 'updatedat'])

        return df
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            conn.close()            
            
def update_table_equipamentos_sus_por_tipos(df,table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        for _, row in df.iterrows():
            codmun = row['codmun']
            ano = row['ano']
            mes = row['mes']
            tipoequipamento = row['tipoequipamento']
            quantidadeequipamentos = row['quantidadeequipamentos']
            
            # Montar o comando SQL para atualização
            update_query = f"""
            UPDATE {table_name}
            SET quantidadeequipamentos = %s,
                mes = %s,
                updatedat = CURRENT_TIMESTAMP - INTERVAL '3 hours'
            WHERE codmun = %s AND ano = %s AND tipoequipamento = %s;
            """
            
            # Executar o comando de atualização
            cur.execute(update_query, (quantidadeequipamentos,mes, codmun, ano,tipoequipamento))
            
        conn.commit()
            
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()    
            
            
def get_ultimo_total_ambulatorio(ano,table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Consulta para obter as colunas
        query = f"""
        select sum(quantidadeaprovadatotal) from {table_name} where ano = {ano}
        """
        
        # Executando a consulta
        cur.execute(query)
        
        resultados = cur.fetchall()
        
        return resultados[0][0]
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            cur.close()
            conn.close()            
            

def get_saude_prod_ambula_qtd_aprovada(ano,table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        
        query = f"""
        select * from {table_name} where ano = {ano};
        """
        
        df = pd.read_sql_query(query, conn)
        
        if df.shape[0]:
            df = df.drop(columns=['createdat', 'updatedat'])

        return df
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            conn.close()  

def update_table_saude_prod_ambula_qtd_aprovada(df,table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        for _, row in df.iterrows():
            codmun = row['codmun']
            ano = row['ano']
            nova_quantidade = row['quantidadeaprovadatotal']
            novo_mes = row['mes']
            # Montar o comando SQL para atualização
            update_query = f"""
            UPDATE {table_name}
            SET quantidadeaprovadatotal = %s, mes = %s, updatedat = CURRENT_TIMESTAMP - INTERVAL '3 hours'
            WHERE codmun = %s AND ano = %s;
            """
            
            # Executar o comando de atualização
            cur.execute(update_query, (nova_quantidade, novo_mes,codmun, ano))
            
        conn.commit()
            
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()               
       

def update_table_percentual_imoveis_assentamentos(df,table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        update_query = f"""
            UPDATE {table_name}
            SET areamunkm2 = %s,
                percentual = %s,
                areaassentamentoskm2 = %s
                updatedat = CURRENT_TIMESTAMP - INTERVAL '3 hours'
            WHERE codmun = %s
        """

        # Criar uma lista de tuplas com os valores do dataframe
        values_to_update = [
            (row['areamunkm2'], row['percentual'], row['areaassentamentoskm2'], row['codmun'])
            for index, row in df.iterrows()
        ]

        # Executar a atualização no banco de dados usando execute_batch para múltiplas linhas
        with conn.cursor() as cursor:
            execute_batch(cursor, update_query, values_to_update)

        # Commit para garantir que as mudanças sejam salvas
        conn.commit()

        # Fechar a conexão
        conn.close()

        print("Banco de dados atualizado com sucesso!")
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            cur.close()
            conn.close()
            
            
def get_table_percentual_imoveis_assentamentos(table_name):
    # Conectando ao banco de dados
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Obtendo os nomes das colunas da tabela
        query_colunas = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}';
        """
        
        # Executando a consulta para obter as colunas
        cur.execute(query_colunas)
        colunas = [row[0] for row in cur.fetchall()]
        
       
        
        # Consulta para obter os dados da tabela
        query_dados = f"SELECT {', '.join(colunas)} FROM {table_name};"
        
        # Executando a consulta para obter os dados
        cur.execute(query_dados)
        dados = cur.fetchall()
        
        # Criando o DataFrame com os dados e as colunas
        df = pd.DataFrame(dados, columns=colunas)
        df = df.drop(columns=['updatedat', 'createdat'])
        return df
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            cur.close()
            conn.close()            
            
            
            
def get_table_percentual_terras_indigenas(table_name):
    # Conectando ao banco de dados
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Obtendo os nomes das colunas da tabela
        query_colunas = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}';
        """
        
        # Executando a consulta para obter as colunas
        cur.execute(query_colunas)
        colunas = [row[0] for row in cur.fetchall()]
        
        #print(colunas)
        
        # Consulta para obter os dados da tabela
        query_dados = f"SELECT {', '.join(colunas)} FROM {table_name};"
        
        # Executando a consulta para obter os dados
        cur.execute(query_dados)
        dados = cur.fetchall()
        
        # Criando o DataFrame com os dados e as colunas
        df = pd.DataFrame(dados, columns=colunas)
        df = df.drop(columns=['updatedat', 'createdat'])
        return df
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            cur.close()
            conn.close()            
            
            
def update_table_percentual_terras_indigenas(df,table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        update_query = f"""
            UPDATE {table_name}
            SET areamunkm2 = %s,
                percentual = %s,
                areainterseccaokm2 = %s,
                terrainome = %s,
                updatedat = CURRENT_TIMESTAMP - INTERVAL '3 hours'
            WHERE terraicod = '%s' AND codmun = %s
        """

        # Criar uma lista de tuplas com os valores do dataframe
        values_to_update = [
            (row['areamunkm2'], row['percentual'], row['areainterseccaokm2'], row['terrainome'], row['terraicod'], row['codmun'])
            for index, row in df.iterrows()
        ]

        # Executar a atualização no banco de dados usando execute_batch para múltiplas linhas
        with conn.cursor() as cursor:
            execute_batch(cursor, update_query, values_to_update)

        # Commit para garantir que as mudanças sejam salvas
        conn.commit()

        # Fechar a conexão
        conn.close()

        print("Banco de dados atualizado com sucesso!")
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            cur.close()
            conn.close()            
            

def get_table_percentual_areas_protegidas(table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        
        query = f"""
        select * from {table_name};
        """
        
        df = pd.read_sql_query(query, conn)
        
        if df.shape[0]:
            df = df.drop(columns=['createdat','updatedat'])

        return df
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            conn.close()            
            
            
def update_table_percentual_areas_protegidas(df,table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                # Create a tuple of values for the SQL execution
                params = (row['areamunkm2'], row['areasprotegidaskm2'], row['percentual'], row['codmun'])

                # Check if params is correctly structured as a tuple/list
                sql = f"""
                UPDATE {table_name}
                SET areamunkm2 = %s, areasprotegidaskm2 = %s, percentual = %s , updatedat = CURRENT_TIMESTAMP - INTERVAL '3 hours'
                WHERE   codmun = %s;
                """

                # Execute the batch update using execute_batch
                execute_batch(cur, sql, [params])  # Pass a list of one tuple

            # Commit the transaction after executing all updates
            conn.commit()
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()            
            
            
def get_table_conflitos(table_name):
    try:
        conn = psycopg2.connect(**conn_params)
        
        query = f"""
        select * from {table_name};
        """
        
        df = pd.read_sql_query(query, conn)
        
        if df.shape[0]:
            df = df.drop(columns=['createdat', 'updatedat'])

        return df
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            conn.close()            

def codmun_siglauf():
    # Conectando ao banco de dados
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Consulta para obter as colunas
        query = f"""
        select codmun, codmun6,nome_sigla, nome, siglauf from municipios_2022;
        """
        
        # Executando a consulta
        cur.execute(query)
        
        resultados = cur.fetchall()
        # Criando o DataFrame com os dados
        df = pd.DataFrame(resultados, columns=['codmun', 'codmun6','nome_sigla', 'nome', 'siglauf'])
        return df
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            cur.close()
            conn.close()         


def update_data_ultima_coleta(rastreabilidade_name, table_name):
    """
    Atualiza a coluna 'data_da_ultima_coleta' na tabela 'rastreabilidade_seguranca_publica_teste'
    com a data de hoje + 1 dia.

    :param rastreabilidade_name: Nome da rastreabilidade (coluna 'nome' na tabela)
    :param table_name: Nome da tabela que será atualizada (coluna 'nome_tabela')
    """
    try:
        # Criando a conexão com o banco
        url = f"postgresql://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}"
        engine = create_engine(url)

        # Criando a sessão
        Session = sessionmaker(bind=engine)
        session = Session()

        metadata = MetaData()
        metadata.reflect(bind=engine)
        tabela = metadata.tables[rastreabilidade_name]
        print("TABELA>>>>>>>>>>", tabela)

        # Gerando a data de hoje + 1 dia
        # data_ultima_coleta = (datetime.now() + timedelta(days=1)).strftime('%d/%m/%Y')
        data_ultima_coleta = (datetime.now()).strftime('%d/%m/%Y')
        data_ultima_coleta = str(data_ultima_coleta)

        # Debug: Verificar se há registros correspondentes antes do UPDATE
        query_check = session.query(tabela).filter(
            tabela.c.nome_tabela == table_name
        ).all()

        if not query_check:
            print(f"⚠️ Nenhum registro encontrado para '{rastreabilidade_name}' na tabela '{table_name}'.")
            session.close()
            engine.dispose()
            return

        # Query de update filtrando pelo nome e nome da tabela
        stmt = (
            update(tabela)
            .where(tabela.c.nome_tabela == table_name)
            .values(data_da_ultima_coleta=data_ultima_coleta)
        )

        # Executando a query e verificando a quantidade de registros atualizados
        result = session.execute(stmt)
        session.commit()

        affected_rows = result.rowcount
        if affected_rows == 0:
            print(f"⚠️ Nenhuma linha foi atualizada. Verifique os filtros!")
        else:
            print(f"✅ Atualizado 'data_da_ultima_coleta' para {data_ultima_coleta} na tabela '{table_name}' com rastreabilidade '{rastreabilidade_name}'. Registros afetados: {affected_rows}")

    except Exception as e:
        print(f"❌ Erro ao atualizar a data: {e}")

    finally:
        session.close()
        engine.dispose()

def get_codmun():
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Consulta para obter as colunas
        query = f"""
        select codmun,nomemuni, siglaestado from table_geocode;
        """
        # Executando a consulta
        cur.execute(query)
        
        # Obtendo os resultados
        
        resultados = cur.fetchall()

        # Definir os nomes das colunas para o DataFrame
        colunas = ['codmun', 'nomemuni', 'siglaestado']

        # Criar o DataFrame a partir dos resultados da consulta
        df = pd.DataFrame(resultados, columns=colunas)
                
        return df
        
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados ou consultar a tabela: {error}")

    finally:
        # Fechando a conexão
        if conn:
            cur.close()
            conn.close()
