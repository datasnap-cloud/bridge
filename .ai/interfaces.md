# Interfaces - DataSnap Bridge

## CLI Commands (`cli.py`)
- `bridge setup`: Wizard de configuração interativa.
- `bridge sync`: Comando principal de sincronização.
    - Flags: `--all`, `--mapping <name>`, `--dry-run`, `--force`, `--status`.
- `bridge status`: Diagnóstico de conectividade e estado.
- `bridge version`: Exibe versão.
- `bridge test_laravel_log`: Comando utilitário para teste de parser de logs.

## API Integration (`datasnap/api.py`)
Interage com o Backend DataSnap (`datasnap-backend`):
- `GET /api/v1/schemas`: Lista schemas disponíveis para o Tenant.
- `POST /api/v1/schemas/{slug}/generate-upload-token`: Obtém URL presigned para upload.
- `POST /api/v1/schemas/{slug}/notify-upload`: (Opcional) Notifica backend se não usar EventBridge.

## Data Formats
- **Input**:
    - **SQL**: Result sets de queries (List of Dicts).
    - **Log**: Linhas de texto não estruturado -> Regex -> Dict.
- **Output**:
    - **JSONL**: Newline Delimited JSON. Compressão GZIP opcional mas recomendada.
    - **Metadata**: Cabeçalhos HTTP para o upload (Content-Type, Metadata customizado).
