import os
import pandas as pd
import time
import requests
import io
from dotenv import load_dotenv
from sqlalchemy import create_engine

# credenciais de banco, lista de links de acesso as bases, depara de colunas e abas a ignorar
load_dotenv()
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_table = os.getenv("DB_TABLE")

LISTA_URLS = [
    "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_2022.xlsx",
    "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_2023.xlsx",
    "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_2024.xlsx",
    "https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_2025.xlsx"
]
MAPA_DEPARA_2022 = {
    'DATA_COMUNICACAO_BO': 'DATA_REGISTRO',
    'CIDADE': 'NOME_MUNICIPIO',
    'DESCR_TIPOLOCAL': 'DESCR_SUBTIPOLOCAL',
    'DESCR_PERIODO': 'DESC_PERIODO'
}
DICIONARIO_IGNORAR = {
    "2023": "CAMPOS_DA_TABELA_SPDADOS",
    "2025": "Campos da Tabela_SPDADOS"
}


# conexão com banco de dados
def criar_engine_banco():

    try:
        connection_string = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            pass
        print("A conexão foi feita com o banco de dados.")
        return engine
    except Exception as e:
        print(f"Falha ao conectar ao banco de dados: {e}")
        return None

# Extração e trabsformação

def extrair_e_transformar(urls, mapa_depara, dic_ignorar):
    
    print("Iniciando e/t")
    lista_dataframes_final = []

    for url in urls:
        print(f"\nProcessando arquivo: {url}")
        try:
            nome_arquivo = url.split('/')[-1]
            response = requests.get(url)
            response.raise_for_status()        
            arquivo_em_memoria = io.BytesIO(response.content)        
            dicionario_de_abas = pd.read_excel(arquivo_em_memoria, sheet_name=None)
            
            for nome_aba, df_aba in dicionario_de_abas.items():

                ignorar_esta_aba = False
                for chave_arquivo, chave_aba in dic_ignorar.items():
                    if chave_arquivo in nome_arquivo and nome_aba == chave_aba:
                        ignorar_esta_aba = True
                        break
                if ignorar_esta_aba:
                    continue
                
                if "2022" in nome_arquivo:
                    df_aba.rename(columns=mapa_depara, inplace=True)

                df_aba.columns = (
                df_aba.columns
                .str.normalize('NFKD')
                .str.encode('ascii', errors='ignore')
                .str.decode('utf-8')
                .str.replace(' ', '_')
                .str.replace('-', '_')
                .str.replace(r'[^0-9a-zA-Z_]', '', regex=True)
                .str.lower()
            )

                lista_dataframes_final.append(df_aba)
                
        except Exception as e:
            print(f"Falha ao processar a URL {url}. Erro: {e}")

    if not lista_dataframes_final:
        print("\nNenhum dado para carregar.")
        return None
        
    try:
        print(f"\nUnificando {len(lista_dataframes_final)} planilhas...")
        df_unificado = pd.concat(lista_dataframes_final, ignore_index=True)
        print(f"DataFrame unificado com {df_unificado.shape[0]} linhas e {df_unificado.shape[1]} colunas.")
        return df_unificado
    except Exception as e:
        print(f"Erro ao unificar os DataFrames: {e}")
        return None

# carga de dados
def carregar_dados(df_para_carregar, engine_conexao, nome_tabela):

    if df_para_carregar is None or engine_conexao is None:
        print("Carga abortada por falta de dados ou conexão com banco.")
        return

    try:
        print("Iniciando a inserção dos dados no banco de dados...")
        start_time = time.time()
        
        df_para_carregar.to_sql(
            name=nome_tabela,
            con=engine_conexao,
            if_exists='append',
            index=False,
            chunksize=1000
        )
       
        end_time = time.time()
        print(f"Operação concluída em {end_time - start_time:.2f} segundos.")
        
    except Exception as e:
        print(f"Ocorreu um erro durante a inserção: {e}")


#execução do etl
def main():

    print("iniciando etl")
    

    engine = criar_engine_banco()
    
    if engine:

        df_final = extrair_e_transformar(
            urls=LISTA_URLS, 
            mapa_depara=MAPA_DEPARA_2022, 
            dic_ignorar=DICIONARIO_IGNORAR
        )
        
        if df_final is not None:
            carregar_dados(
                df_para_carregar=df_final, 
                engine_conexao=engine, 
                nome_tabela=os.getenv("DB_TABLE")
            )
            
    print("etl finalizado")

if __name__ == "__main__":
    main()