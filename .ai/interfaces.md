Last verified: 2025-12-28
Source of truth: codebase scan

# Interfaces

## CLI Commands
Evidência: `cli.py`
- `bridge setup`: TUI interativa para configuração de credenciais.
- `bridge sync`:
  - `--mapping`: Nome do mapeamento a sincronizar.
  - `--all`: Todos os mapeamentos.
  - `--parallel / --sequential`: Modo de execução.
  - `--dry-run`: Teste sem upload.
- `bridge test_laravel_log`: Comando utilitário específico para parsing de logs Laravel.
- `bridge status`: Health check de conectividade e chaves.
- `bridge version`: Exibe versão.

## Modules / Internals
Evidência: `sync/`
- `SyncRunner`: Gerencia pool de threads/processos.
- `Extractor`: Lê do banco (MySQL/Postgres) ou Arquivo.
- `JSONLWriter`: Converte e comprime para disco.
- `Uploader`: Envia para API (`POST /api/v1/schemas/{slug}/files`).

## Filesystem
- **Config**: `.env` ou Local Secure Store (OS dependent).
- **Logs**: `bridge.log`.
- **Buffer**: Diretório temporário para JSONL antes do upload.
