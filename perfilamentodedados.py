import pandas as pd
from sqlalchemy import create_engine
import textwrap
from dotenv import load_dotenv
import os

# variáveis de acesso banco (valores das variaveis em arquivo .env)

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_table = os.getenv("DB_TABLE")

# colunas para analisar

colunas_para_analisar = [
    'nome_departamento', 'nome_seccional', 'nome_delegacia', 'nome_municipio',
    'num_bo', 'ano_bo', 'data_registro', 'data_ocorrencia_bo',
    'hora_ocorrencia_bo', 'desc_periodo',
    'descr_subtipolocal', 'bairro',
    'logradouro', 'numero_logradouro', 'latitude', 'longitude', 'nome_delegacia_circunscricao', 'nome_departamento_circunscricao',
    'nome_seccional_circunscricao', 'nome_municipio_circunscricao', 'rubrica','descr_conduta','natureza_apurada','mes_estatistica','ano_estatistica'
]

print("--- INICIANDO SCRIPT DE PERFILAMENTO DE DADOS ---")
print(f"Analisando a tabela: '{db_table}' no banco '{db_name}'\n")

# conexão banco e analise colunas
try:
    connection_string = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        
        for coluna in colunas_para_analisar:
            print(f"--- Analisando a coluna: {coluna} ---")

            # analise de valores nulos (com a string null e realmente null)
            query_perfil = textwrap.dedent(f"""
                SELECT
                    COUNT(DISTINCT `{coluna}`) AS valores_unicos,
                    SUM(CASE WHEN `{coluna}` IS NULL THEN 1 ELSE 0 END) AS nulos_reais,
                    SUM(CASE WHEN `{coluna}` = 'NULL' THEN 1 ELSE 0 END) AS string_null
                FROM {db_table};
            """)

            # perfilamento valores mais frequentes
            query_top_valores = textwrap.dedent(f"""
                SELECT `{coluna}`, COUNT(*) as quantidade
                FROM {db_table}
                GROUP BY `{coluna}`
                ORDER BY quantidade DESC
                LIMIT 10;
            """)

            
            df_perfil = pd.read_sql(query_perfil, connection)
            print("Relatório de Qualidade:")
            print(df_perfil.to_string(index=False))
            
            print("\nTop 10 Valores Mais Frequentes:")
            df_top_valores = pd.read_sql(query_top_valores, connection)
            print(df_top_valores.to_string(index=False))
            
            print("-" * 50 + "\n")

except Exception as e:
    print(f"ERRO: Ocorreu um problema durante a execução: {e}")

finally:
    print("--- SCRIPT DE PERFILAMENTO CONCLUÍDO ---")