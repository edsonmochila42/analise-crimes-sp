# Análise de Criminalidade (SSP-SP) - Projeto de Portfólio End-to-End

**Status do Projeto:** Em Desenvolvimento

## 1. Descrição

Este projeto realiza uma análise de dados ponta-a-ponta (E2E) sobre os dados públicos de Boletins de Ocorrência (BO) da Secretaria de Segurança Pública de São Paulo (SSP-SP).

O objetivo principal é construir um pipeline de dados completo, desde a captura automatizada dos dados brutos até a criação de um dashboard interativo em Power BI para visualização e geração de insights.

## 2. Arquitetura do Pipeline de Dados (ETL)

O projeto foi estruturado para simular um ambiente de dados corporativo, com fases claras de Extração, Transformação e Carga (ETL).

1.  **Extração (E):** Um script Python (`conexaocombanco.py`) é responsável por ler múltiplos arquivos Excel (`.xlsx`) de diferentes anos, disponibilizados pela SSP.
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
python -m venv venv
.\venv\Scripts\activate

# 3. Instale as dependências
pip install -r requirements.txt 
# (OBS: Você precisará criar este arquivo requirements.txt)

# 4. Configure seu "cofre" de senhas
# Crie um arquivo .env na raiz do projeto com a seguinte estrutura:
DB_USER=seu_usuário
DB_PASSWORD=sua_senha_do_banco
DB_HOST=seu_local
DB_PORT=sua_porta
DB_NAME=ssp_crimes_db
DB_TABLE=ocorrencias
