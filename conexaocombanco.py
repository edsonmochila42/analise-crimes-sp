import os
import pandas as pd
import time
import os
import requests
import io
from dotenv import load_dotenv
from sqlalchemy import create_engine

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
    print(f"Falha ao conectar ao banco de dados: {e}")
    exit()


# url de dados a serem extraidos

lista_urls = [
    "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_2022.xlsx",
    "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_2023.xlsx",
    "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_2024.xlsx",
    "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_2025.xlsx"
]

# transformação prévia de dados (4 colunas de 2022 não seguem o padrão dos demais anos, esse trecho padroniza esse atributos)

arquivo_com_depara = "SPDadosCriminais_2022"
mapa_depara = {
    'DATA_COMUNICACAO_BO': 'DATA_REGISTRO',
    'CIDADE': 'NOME_MUNICIPIO',
    'DESCR_TIPOLOCAL': 'DESCR_SUBTIPOLOCAL',
    'DESCR_PERIODO': 'DESC_PERIODO'
}
 
# no arquivo de 2023 tem uma aba que é um dicionário de dados e precisa ser ignorada.
arquivo_com_aba_ignorar = "SPDadosCriminais_2023"
aba_para_ignorar = "CAMPOS_DA_TABELA_SPDADOS"


print("Iniciando processo de Extração e Transformação (E/T)...")
lista_dataframes_final = []

for url in lista_urls:
    print(f"\nProcessando arquivo: {url}")
    
    try:
        # nome do arquivo da URL para usar nos filtros
        nome_arquivo = url.split('/')[-1]

        response = requests.get(url)
        response.raise_for_status()
        
        arquivo_em_memoria = io.BytesIO(response.content)
        
        print(f"  -> Lendo abas do arquivo...")
        dicionario_de_abas = pd.read_excel(arquivo_em_memoria, sheet_name=None)
        print(f"  -> Encontradas {len(dicionario_de_abas)} abas.")

        for nome_aba, df_aba in dicionario_de_abas.items():
            
            # ignora aba de 2023
            if arquivo_com_aba_ignorar in nome_arquivo and nome_aba == aba_para_ignorar:
                continue # Pula para a próxima aba

            # depara de colunas arquivo de 2022
            if arquivo_com_depara in nome_arquivo:
                print(f"    -> [aplicando depara 2022] Aba: {nome_aba}")
                df_aba.rename(columns=mapa_depara, inplace=True)
            
            print(f"    -> [processando] Aba: {nome_aba}")
            lista_dataframes_final.append(df_aba)

    except Exception as e:
        print(f"Falha ao processar a URL {url}. Erro: {e}")

print("\n...Processo de E/T concluído.")


# unificando arquivos
df_unificado = None
if lista_dataframes_final:
    try:
        print(f"\nUnificando {len(lista_dataframes_final)} planilhas...")
        df_unificado = pd.concat(lista_dataframes_final, ignore_index=True)
        print(f"DataFrame unificado com {df_unificado.shape[0]} linhas e {df_unificado.shape[1]} colunas.")
    except Exception as e:
        print(f"erro ao unificar os DataFrames: {e}")
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
        start_time = time.time()
        df_unificado.to_sql(
            name=db_table,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )
       
        end_time = time.time()
        tempo_total = end_time - start_time
        
        print("Todos os dados foram inseridos na tabela 'ocorrencias_sp_ssp'.")
        print(f"Operação concluída em {tempo_total:.2f} segundos.")
        
    except Exception as e:
        print("Ocorreu um erro durante a inserção no banco de dados.")
        print(f"Erro: {e}")