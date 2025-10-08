# Bridge (DataSnap)

**Bridge** é o utilitário **oficial** da DataSnap para **migração e transferência automática de dados**.  
Feito para squads que querem começar **em minutos**: CLI pronta, templates de configuração e conectores para
**filesystem local**, **S3/compatível** e **OCI Object Storage** (com suporte incremental no roadmap).

---

## ✨ Por que usar o Bridge?

- **Comece rápido**: gere um `bridge.yaml` de exemplo e rode migrações em minutos.
- **Pense em fluxo, não em plumbing**: CLI simples, logs legíveis e *dry‑run*.
- **Pronto para DataSnap**: caminhos, convenções e validações pensadas para ingestão na plataforma.

---

## 🚀 Instalação (dev)

### Pré-requisitos
- **Python ≥ 3.9**
- **pip** (gerenciador de pacotes Python)
- **Acesso à API DataSnap** (API Key necessária)

### Instalação do Ambiente

#### 1. Clone o repositório
```bash
git clone <repository-url>
cd datasnap-bridge
```

#### 2. Crie um ambiente virtual
```bash
python -m venv venv

# Linux/macOS
source venv/bin/activate

# Windows
venv\Scripts\activate
```

#### 3. Instale as dependências
```bash
pip install -r requirements.txt
```

#### 4. Configuração inicial
```bash
# Execute o setup interativo
python -m bridge setup

# Ou verifique o status
python -m bridge status
```

### Dependências Principais

O Bridge utiliza as seguintes bibliotecas principais:
- **`cryptography`** - Criptografia AES-GCM para dados sensíveis
- **`requests`** - Cliente HTTP para comunicação com API DataSnap
- **`psycopg2-binary`** - Conector PostgreSQL
- **`PyMySQL`** - Conector MySQL
- **`rich`** - Interface de terminal rica e colorida
- **`asyncio`** - Processamento assíncrono para performance

### Estrutura do Projeto

```
datasnap-bridge/
├── bridge/                 # Módulo principal
│   ├── __main__.py        # Entry point CLI
│   └── cli.py             # Comandos CLI
├── core/                  # Funcionalidades core
│   ├── crypto.py          # Criptografia e segurança
│   ├── datasources_store.py # Gerenciamento de fontes
│   ├── http.py            # Cliente HTTP
│   ├── paths.py           # Gerenciamento de caminhos
│   └── secrets_store.py   # Armazenamento seguro
├── setup/                 # Interface de configuração
│   ├── menu.py            # Menu principal TUI
│   └── validators.py      # Validadores de entrada
├── sync/                  # Motor de sincronização
│   ├── runner.py          # Orquestrador principal
│   ├── extractor.py       # Extração de dados
│   ├── uploader.py        # Upload para DataSnap
│   └── metrics.py         # Coleta de métricas
├── tests/                 # Testes automatizados
└── .bridge/              # Dados de configuração
    ├── config/           # Configurações gerais
    ├── state/            # Estado das sincronizações
    └── logs/             # Logs de execução
```

---

## 🏁 Começo rápido

### 1. Configuração Inicial
```bash
# Execute o menu de configuração
python -m bridge setup

# Siga os passos:
# 1. Cadastre sua API Key da DataSnap
# 2. Configure suas fontes de dados (MySQL/PostgreSQL)
# 3. Valide as conexões
```

### 2. Primeira Sincronização
```bash
# Verifique o status do sistema
python -m bridge status

# Execute uma sincronização de teste
python -m bridge sync --dry-run

# Execute sincronização real
python -m bridge sync
```

### 3. Monitoramento
```bash
# Acompanhe os logs em tempo real
tail -f .bridge/logs/sync.log

# Verifique o estado das sincronizações
cat .bridge/state/sync_state.json
```

---

## 🔧 Configuração

### Configuração de API Keys

As API Keys da DataSnap são armazenadas de forma criptografada em `.bridge/api_keys.enc`:

```bash
# Adicionar nova API Key via menu
python -m bridge setup
# Selecione: "Gerenciar API Keys" > "Cadastrar nova API Key"

# Listar API Keys cadastradas
python -m bridge setup
# Selecione: "Gerenciar API Keys" > "Listar API Keys"
```

### Configuração de Fontes de Dados

#### MySQL
```json
{
  "name": "mysql-prod",
  "type": "mysql",
  "connection": {
    "host": "localhost",
    "port": 3306,
    "database": "production",
    "username": "bridge_user",
    "password": "secure_password"
  }
}
```

#### PostgreSQL
```json
{
  "name": "postgres-analytics",
  "type": "postgresql", 
  "connection": {
    "host": "analytics.company.com",
    "port": 5432,
    "database": "analytics",
    "username": "readonly_user",
    "password": "readonly_pass"
  }
}
```

### Configuração de Mapeamentos

Os mapeamentos definem como os dados são extraídos e enviados para a DataSnap:

```json
{
  "version": "1.0",
  "source": {
    "connection_ref": "mysql-prod",
    "table": "user_events",
    "schema": "analytics"
  },
  "table": "user_events",
  "schema": "production",
  "schema_slug": "prod.user_events",
  "transfer": {
    "batch_size": 10000,
    "max_file_size_mb": 100,
    "retry_attempts": 3,
    "min_records_for_upload": 0
  },
  "query": "SELECT * FROM user_events WHERE created_at > '2024-01-01'"
}
```

#### Configurações de Transfer

A seção `transfer` contém configurações específicas para o processo de sincronização:

- **`batch_size`** (padrão: 5000): Número de registros processados por lote
- **`max_file_size_mb`** (padrão: 100): Tamanho máximo dos arquivos JSONL em MB
- **`retry_attempts`** (padrão: 3): Número de tentativas em caso de falha no upload
- **`min_records_for_upload`** (padrão: 0): Número mínimo de registros necessários para realizar o upload
  - Se o número de registros extraídos for menor que este valor, o upload será cancelado
  - Útil para evitar uploads desnecessários quando há poucos dados novos
  - Valor 0 significa que o upload sempre será realizado, independente da quantidade de registros
```

### Variáveis de Ambiente

```bash
# Nível de log (DEBUG, INFO, WARNING, ERROR)
export BRIDGE_LOG_LEVEL=INFO

# Diretório de configuração customizado
export BRIDGE_CONFIG_DIR=/custom/path/.bridge

# Timeout para operações HTTP (segundos)
export BRIDGE_HTTP_TIMEOUT=30

# Modo dry-run global
export BRIDGE_DRY_RUN=true
```

---

## 📋 Comandos Disponíveis

O Bridge oferece os seguintes comandos CLI:

### `bridge sync`
Executa a sincronização de dados de acordo com os mapeamentos configurados.

Este comando realiza a extração, transformação e carregamento (ETL) de dados das fontes configuradas para a DataSnap:
- **Extrai dados** das fontes de dados configuradas (MySQL, PostgreSQL)
- **Processa e valida** os dados de acordo com os mapeamentos
- **Carrega os dados** na plataforma DataSnap
- **Monitora métricas** e estado da sincronização
- **Suporte a execução paralela** para múltiplos mapeamentos

#### Opções disponíveis:
```bash
# Sincronizar um mapeamento específico
python -m bridge sync mapping_name

# Sincronizar múltiplos mapeamentos
python -m bridge sync mapping1 mapping2 mapping3

# Sincronizar todos os mapeamentos disponíveis
python -m bridge sync --all

# Execução em modo dry-run (sem upload)
python -m bridge sync --dry-run mapping_name

# Forçar sincronização completa (ignorar estado anterior)
python -m bridge sync --force mapping_name

# Execução sequencial (não paralela)
python -m bridge sync --no-parallel mapping1 mapping2
```

#### Configuração de Mapeamentos
Os mapeamentos são definidos em arquivos JSON na pasta `.bridge/config/mappings/`:

```json
{
  "version": "1.0",
  "source": {
    "connection_ref": "local",
    "table": "tenant_logs",
    "schema": "datasnap"
  },
  "table": "tenant_logs",
  "schema": "local",
  "schema_slug": "local.tenant_logs",
  "transfer": {
    "batch_size": 10000,
    "max_file_size_mb": 100
  }
}
```

#### Monitoramento e Logs
- **Estado da sincronização** é salvo em `.bridge/state/sync_state.json`
- **Métricas detalhadas** são coletadas durante a execução
- **Logs estruturados** com níveis DEBUG, INFO, WARNING, ERROR
- **Suporte a retry automático** em caso de falhas temporárias

```bash
python -m bridge sync
```

### `bridge setup`
Menu interativo para configurar API Keys, Fontes de Dados e consultar Schemas da DataSnap.

Este comando abre um menu TUI (Terminal User Interface) que permite:
- **Cadastrar e validar API Keys** da DataSnap
- **Listar API Keys cadastradas** (com tokens mascarados para segurança)
- **Gerenciar Fontes de Dados** (MySQL e PostgreSQL)
  - Criar novas conexões de banco de dados
  - Validar conectividade antes de salvar
  - Listar fontes cadastradas
  - Cadastrar tabelas de uma fonte específica
  - Excluir fontes de dados
- **Consultar Modelos de Dados (Schemas)** da API
- **Gerenciar configurações** de forma segura

Todos os dados sensíveis são criptografados com AES-GCM e armazenados localmente.

```bash
python -m bridge setup
```

### `bridge status`
Exibe o status do sistema e conectividade com a API DataSnap.

Mostra informações sobre:
- **Número de API Keys cadastradas**
- **Status da conectividade** com a API DataSnap
- **Informações do sistema**

```bash
python -m bridge status
```

### `bridge version`
Exibe informações sobre a versão atual do Bridge.

```bash
python -m bridge version
```

---

## ⏰ Agendamento Automático (Cron Job)

Para executar sincronizações automaticamente em intervalos regulares, você pode configurar um cron job no sistema.

### Configuração do Cron Job

#### 1. Editar o crontab
```bash
crontab -e
```

#### 2. Adicionar a linha de agendamento
```bash
# Executar sincronização a cada 5 minutos
*/5 * * * * /usr/bin/env bash -lc 'cd /opt/datasnap-bridge && venv/bin/bridge sync >> .bridge/logs/sync.log 2>&1'

# Executar sincronização diariamente às 02:00
0 2 * * * /usr/bin/env bash -lc 'cd /opt/datasnap-bridge && venv/bin/bridge sync --all >> .bridge/logs/sync.log 2>&1'

# Executar sincronização a cada hora
0 * * * * /usr/bin/env bash -lc 'cd /opt/datasnap-bridge && venv/bin/bridge sync >> .bridge/logs/sync.log 2>&1'
```

#### 3. Explicação dos componentes:
- **`*/5 * * * *`** - A cada 5 minutos
- **`/usr/bin/env bash -lc`** - Executa bash com perfil completo carregado
- **`cd /opt/datasnap-bridge`** - Navega para o diretório do Bridge
- **`venv/bin/bridge sync`** - Executa o comando de sincronização
- **`>> .bridge/logs/sync.log 2>&1`** - Redireciona logs para arquivo

### Configurações Recomendadas

#### Para ambientes de produção:
```bash
# Sincronização incremental a cada 15 minutos
*/15 * * * * /usr/bin/env bash -lc 'cd /opt/datasnap-bridge && venv/bin/bridge sync >> .bridge/logs/sync.log 2>&1'

# Sincronização completa diária (com --force)
0 3 * * * /usr/bin/env bash -lc 'cd /opt/datasnap-bridge && venv/bin/bridge sync --all --force >> .bridge/logs/sync-full.log 2>&1'
```

#### Para ambientes de desenvolvimento:
```bash
# Sincronização a cada hora durante horário comercial
0 9-18 * * 1-5 /usr/bin/env bash -lc 'cd /opt/datasnap-bridge && venv/bin/bridge sync >> .bridge/logs/sync.log 2>&1'
```

### Monitoramento dos Logs

#### Visualizar logs em tempo real:
```bash
tail -f .bridge/logs/sync.log
```

#### Verificar últimas execuções:
```bash
tail -n 100 .bridge/logs/sync.log
```

#### Rotação de logs (logrotate):
Crie o arquivo `/etc/logrotate.d/datasnap-bridge`:
```
/opt/datasnap-bridge/.bridge/logs/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 bridge bridge
}
```

### Variáveis de Ambiente

Para configurações específicas no cron, você pode definir variáveis de ambiente:

```bash
# No início do crontab
BRIDGE_LOG_LEVEL=INFO
BRIDGE_CONFIG_DIR=/opt/datasnap-bridge/.bridge

# Cron job com configurações específicas
*/5 * * * * /usr/bin/env bash -lc 'export BRIDGE_LOG_LEVEL=DEBUG && cd /opt/datasnap-bridge && venv/bin/bridge sync >> .bridge/logs/sync.log 2>&1'
```

### Verificação do Status

Para verificar se o cron job está funcionando:

```bash
# Verificar se o cron está rodando
sudo systemctl status cron

# Verificar logs do cron
sudo tail -f /var/log/cron

# Listar cron jobs ativos
crontab -l
```

---

## 🧪 Exemplos de uso

### Local → Local (MVP)
```bash
a fazer
```

## 🧠 Dicas de performance (gerais)

- Prefira **arquivos de até 10MB** para melhor performance de ingestão de dados.
- Evite milhões de pequenos arquivos.

---

## 🧪 Testes Automatizados

O Bridge possui uma suíte abrangente de testes automatizados para garantir a qualidade e confiabilidade do código.

### Executando os Testes

#### Testes Unitários (pytest)
```bash
# Executar todos os testes unitários
python -m pytest tests/

# Executar testes com cobertura
python -m pytest tests/ --cov=core --cov=setup

# Executar um teste específico
python -m pytest tests/test_crypto.py
```



### Estrutura dos Testes

#### Testes Unitários (`tests/`)
- **`test_crypto.py`** - Testes de criptografia AES-GCM e derivação de chaves
- **`test_database_validators.py`** - Validação de conexões MySQL e PostgreSQL
- **`test_datasources_store.py`** - Gerenciamento de fontes de dados
- **`test_http.py`** - Cliente HTTP e comunicação com API
- **`test_paths.py`** - Gerenciamento de caminhos e diretórios
- **`test_secrets_store.py`** - Armazenamento seguro de credenciais

#### Testes Funcionais (raiz do projeto)
- **`test_full_flow.py`** - Teste completo do fluxo de configuração
- **`test_statistics.py`** - Estatísticas otimizadas sem validação de API
- **`test_menu_statistics.py`** - Menu principal com estatísticas
- **`test_schema_pagination.py`** - Paginação na listagem de schemas
- **`test_option4*.py`** - Vinculação de tabelas com schemas
- **`test_datasources.py`** - Gerenciamento de fontes de dados
- **`test_with_tables.py`** - Fluxo com tabelas simuladas

#### Demos e Exemplos
- **`demo_schema_pagination.py`** - Demonstração interativa da paginação
- **`demo_statistics.py`** - Demonstração das estatísticas otimizadas

### Cobertura de Testes

Os testes cobrem:
- ✅ **Criptografia e segurança** - AES-GCM, Argon2id, nonces únicos
- ✅ **Conectividade de banco** - MySQL, PostgreSQL, validação de credenciais
- ✅ **API DataSnap** - Autenticação, listagem de schemas, tratamento de erros
- ✅ **Interface de usuário** - Menus, paginação, entrada do usuário
- ✅ **Armazenamento de dados** - Fontes de dados, API keys, cache de schemas
- ✅ **Fluxos completos** - Configuração end-to-end, vinculação de tabelas

### Executando Testes em CI/CD

```bash
# Script para CI/CD
python -m pytest tests/ --cov=core --cov=setup --cov-report=xml --cov-report=html
```

---

## 🤝 Contribuindo

- Abra issues com **casos reais de migração** (tamanho, origem/destino, volume de arquivos).
- Pull requests bem-vindos — mantenha estilo dos módulos existentes e cobertura básica de testes.
- **Sempre adicione testes** para novas funcionalidades ou correções de bugs.

---

## ❓ FAQ

**Bridge reprocessa arquivos já migrados?**  
Por padrão, vamos incluir **checks de idempotência** no roadmap (hash/etag/size).

**Preciso do PyArrow para começar?**  
Não. Parquet e row‑count são opcionais (apenas se você quiser validações/transformações).

**Quais bancos de dados são suportados?**  
Atualmente: **MySQL** e **PostgreSQL**. Mais conectores no roadmap.

**Como funciona a validação de conexão?**  
O Bridge testa a conectividade executando `SELECT 1` antes de salvar as credenciais.

**Posso selecionar tabelas específicas?**  
Sim! Após cadastrar uma fonte, use "Cadastrar tabelas" para descobrir e selecionar tabelas específicas.

---

## 🔒 Segurança

- Nenhum segredo é commitado. Use `.env` (git‑ignored) ou **store seguro** (ex.: OCI Vault, Secrets Manager).  
- Logs não imprimem segredos.
- **Criptografia forte**: Todos os dados sensíveis (API Keys, credenciais de banco) são criptografados com **AES-GCM**.
- **Chaves derivadas**: Utiliza **Argon2id** para derivação de chaves baseada no `machine-id` do sistema.
- **Permissões restritivas**: Arquivos `.enc` são criados com permissões `0o600` (apenas proprietário).
- **Nonces únicos**: Cada operação de criptografia utiliza um nonce aleatório de 12 bytes.

### Arquivos de Configuração

O Bridge armazena dados na pasta `.bridge/` ao lado do executável:

- `.bridge/config.json` - Configurações gerais (não criptografado)
- `.bridge/api_keys.enc` - API Keys da DataSnap (criptografado)
- `.bridge/datasources.enc` - Credenciais de fontes de dados (criptografado)

---

## 📜 Licença

MIT © DataSnap

---

## 🧭 Suporte

- Dúvidas de onboarding/DataSnap: entre em contato com o time DataSnap.
- Problemas no Bridge: abra uma issue com logs (`BRIDGE_LOG_LEVEL=DEBUG`).

