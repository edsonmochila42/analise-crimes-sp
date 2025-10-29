import os
import pandas as pd
from sqlalchemy import create_engine
import time
import os
from dotenv import load_dotenv

print("Iniciando o script de carga de dados...")

# credenciais do banco salvas arquivo .env.

load_dotenv()

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_table = os.getenv("DB_TABLE")


try:
    connection_string = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        pass
    print("Conexão com o banco de dados foi bem-sucedida.")
except Exception as e:
    print(f"ERRO: Falha ao conectar ao banco de dados: {e}")
    exit()

# leituras dos arquivos em excel

print("\nIniciando a leitura de todos os arquivos Excel...")
lista_de_dataframes = []
caminho_da_pasta = r"C:\Users\edson\OneDrive\Desktop\python_work\projeto_powerbi"
try:
    arquivos_excel = [f for f in os.listdir(caminho_da_pasta) if f.endswith(('.xlsx', '.xls'))]
    if not arquivos_excel:
        print("Nenhum arquivo Excel encontrado na pasta.")
    else:
        for nome_arquivo in arquivos_excel:
            caminho_completo = os.path.join(caminho_da_pasta, nome_arquivo)
            planilhas_do_arquivo = pd.read_excel(caminho_completo, sheet_name=None)
            for nome_planilha, df in planilhas_do_arquivo.items():
                lista_de_dataframes.append(df)
except Exception as e:
    print(f"\nERRO ao processar os arquivos Excel: {e}")

# unificando arquivos
df_unificado = None
if lista_de_dataframes:
    try:
        print(f"\nUnificando {len(lista_de_dataframes)} planilhas...")
        df_unificado = pd.concat(lista_de_dataframes, ignore_index=True)
        print(f">>> SUCESSO! DataFrame unificado com {df_unificado.shape[0]} linhas e {df_unificado.shape[1]} colunas.")
    except Exception as e:
        print(f"ERRO ao unificar os DataFrames: {e}")
else:
    print("\nNenhum dado para carregar. O script será encerrado.")
    exit()

# normalizando nome das colunas

df_unificado.columns = (
    df_unificado.columns
    .str.normalize('NFKD')
    .str.encode('ascii', errors='ignore')
    .str.decode('utf-8')
    .str.replace(' ', '_')
    .str.replace('-', '_')
    .str.replace(r'[^0-9a-zA-Z_]', '', regex=True)
    .str.lower()
)


# carregando dados no banco
if df_unificado is not None:
    try:
        
        print("Iniciando a inserção dos dados no banco de dados...")

        # Inicia a contagem do tempo
        start_time = time.time()

        # O comando para enviar os dados para o SQL
        df_unificado.to_sql(
            name=db_table,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )

       
        end_time = time.time()
        tempo_total = end_time - start_time

        
        print(">>> SUCESSO FINAL! Todos os dados foram inseridos na tabela 'ocorrencias'.")
        print(f"Operação concluída em {tempo_total:.2f} segundos.")
        

    except Exception as e:
        print("\n>>> FALHA! Ocorreu um erro durante a inserção no banco de dados.")
        print(f"Erro: {e}")