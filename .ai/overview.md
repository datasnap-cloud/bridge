Last verified: 2025-12-28
Source of truth: codebase scan

# Overview

## O que é este repo
O **datasnap-bridge** é a ferramenta de linha de comando (CLI) oficial para clientes. Ela orquestra a sincronização de dados locais (bancos de dados, logs) para a nuvem DataSnap de forma segura e eficiente.

## Stack
- **CLI Framework**: Typer (Python).
- **UI**: Rich.
- **Crypto**: Cryptography (fernet?).
- **Conectores**: PyMySQL, Psycopg2.

## Arquitetura
- **Core**: `core/` (Logger, HTTP Client, Secrets Store).
- **Sync**: `sync/` (Extração, Conversão JSONL, Upload).
- **Entrypoint**: `cli.py` (Comandos Typer).

## Fluxos Principais
1.  **Setup**: `bridge setup` (Cadastro de chaves e configuração segura).
2.  **Sync**: `bridge sync` (Leitura de fonte -> JSONL -> Upload Multipart/Presigned).

## Hotspots
- **Runner**: `sync/runner.py`.
- **Extractor**: `sync/extractor.py`.

## Como rodar local
```bash
# Como usuário/dev
pip install -r requirements.txt
python cli.py --help
```
