# Análise de Criminalidade (SSP-SP) - Projeto de Portfólio End-to-End

**Status do Projeto:** Em Desenvolvimento

## 1. Objetivos do Projeto

Este projeto realiza uma análise de dados ponta-a-ponta (E2E) sobre os dados públicos de Boletins de Ocorrência (BO) da Secretaria de Segurança Pública de São Paulo (SSP-SP).

O projeto possui um **duplo objetivo**:

1.  **Engenharia de Dados:** Construir um pipeline de dados (ETL) automatizado. O pipeline é responsável por extrair os dados diretamente do portal da SSP, tratá-los em memória, padronizá-los e carregá-los em um Data Warehouse local (MySQL).
2.  **Análise de Dados (BI):** Utilizar os dados tratados para responder perguntas e gerar insights através de:
    * Uma **análise descritiva e histórica** da evolução das taxas de criminalidade por município.
    * Uma **análise correlacional**, cruzando os dados de crimes com indicadores socioeconômicos de outras fontes (como PIB per capita e perfil demográfico do IBGE).

O resultado final é a visualização dos insights em um dashboard interativo no Power BI.

## 2. Arquitetura do Pipeline de Dados (ETL)

O projeto foi estruturado para simular um ambiente de dados corporativo, com fases claras de Extração, Transformação e Carga (ETL).

1.  **Extração (E):** O script (`conexaocombanco.py`) utiliza a biblioteca **`requests`** para se conectar ao portal da SSP e realizar o download dos arquivos `.xlsx`. Para otimizar o processo e evitar o salvamento de arquivos físicos intermediários no disco, os dados são lidos **diretamente em memória** (utilizando o módulo **`io`** e a classe `BytesIO`) e carregados em um DataFrame Pandas para a próxima etapa.
2.  **Transformação (T):** Utilizando a biblioteca **Pandas**, os dados são:
    * Unificados em um único DataFrame.
    * Padronizados: Nomes de colunas são normalizados (remoção de acentos e caracteres especiais) para se alinharem ao esquema do banco de dados.
3.  **Carga (L):** O DataFrame limpo e unificado é carregado em um Data Warehouse local (`MySQL` via XAMPP) usando **SQLAlchemy**, em *chunks* (lotes) para otimizar a performance de escrita.

### Boas Práticas de Segurança
Todas as credenciais do banco de dados (host, usuário, senha) não estão no código. Elas são gerenciadas de forma segura através de variáveis de ambiente, carregadas a partir de um arquivo `.env` (que é ignorado pelo Git).

## 3. Como Executar o Projeto

### Pré-requisitos
* [Python 3.10+](https://www.python.org/downloads/)
* [Git](https://git-scm.com/downloads)
* [XAMPP](https://www.apachefriends.org/pt_br/index.html) (ou um servidor MySQL/MariaDB)

### 1. Configuração do Ambiente

```bash
# 1. Clone este repositório
git clone [https://github.com/edsonmochila42/analise-crimes-sp.git](https://github.com/edsonmochila42/analise-crimes-sp.git)
cd analise-crimes-sp

# 2. Crie e ative um ambiente virtual (venv)
# No Windows:
python -m venv venv
.\venv\Scripts\activate
# No macOS/Linux:
# python3 -m venv venv
# source venv/bin/activate

# 3. Instale as dependências
pip install -r requirements.txt 

# 4. Configure seu "cofre" de senhas
# Crie um arquivo .env na raiz do projeto com a seguinte estrutura:

DB_USER=seu_usuário
DB_PASSWORD=sua_senha_do_banco
DB_HOST=localhost
DB_PORT=3306
DB_NAME=ssp_crimes_db
DB_TABLE=ocorrencias