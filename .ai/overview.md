# DataSnap Bridge - Context Overview

## O que é este repo
Este é o **DataSnap Bridge**, uma ferramenta CLI (Command Line Interface) em Python para sincronização de dados on-premise ou cloud-to-cloud com a plataforma DataSnap.
- **Tecnologia**: Python 3.10+, Typer (CLI), Rich (UI), HTTPX/Requests.
- **Objetivo**: Conectar em bancos de dados (MySQL, PostgreSQL, SQL Server) ou ler logs (Laravel Log), extrair dados, converter para JSONL e fazer upload seguro para a DataSnap API.
- **Segurança**: Criptografia local de credenciais (`cryptography`) e uso de API Keys.

## Ambientes
| Ambiente | Notas |
| :--- | :--- |
| **CLI Local** | Roda no servidor do cliente ou em container. |
| **Produção** | Distribuído como pacote PyPI ou Docker Image. |

## Módulos Principais
1. **Core (`core/`)**:
    - `crypto.py`: Criptografia de segredos locais.
    - `http.py`: Cliente HTTP resiliente.
    - `logger.py`: Logging centralizado.
    - `secrets_store.py`: Gestão de cofre local.
2. **Sync (`sync/`)**:
    - `extractor.py`: Extrai dados da fonte (SQL/File).
    - `jsonl_writer.py`: Escreve batches em JSONL compactado.
    - `uploader.py`: Envia arquivos para Signed URLs da DataSnap.
    - `runner.py`: Orquestra o loop de sincronização.
3. **API Client (`datasnap/`)**:
    - `api.py`: Wrapper para chamadas à API de gerenciamento da DataSnap.

## Fluxos Principais
- **Setup**: `bridge setup` -> Solicita API Key -> Valida -> Criptografa e salva em `.bridge/secrets.json`.
- **Sync**: `bridge sync` -> Lê configuração de mapeamento -> Extrai Batch -> Grava JSONL -> Pede Upload URL (`POST /schemas/{slug}/generate-upload-token`) -> Faz PUT no S3/Oracle -> Confirma upload.
- **Recovery**: Mantém estado em local store para retomar de onde parou (incremental sync).

## Hotspots (Onde mexer)
- **CLI Entrypoint**: `cli.py` (comandos Typer).
- **Lógica de Extração**: `sync/extractor.py`.
- **Envio de Dados**: `sync/uploader.py`.

## Como Rodar Local
1. `python -m venv venv`
2. `source venv/bin/activate` (ou Windows)
3. `pip install -r requirements.txt`
4. `python cli.py setup`
5. `python cli.py sync --all --dry-run`
