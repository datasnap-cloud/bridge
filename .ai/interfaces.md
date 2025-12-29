Last verified: 2025-12-28
Source of truth: codebase scan

# Interfaces

## CLI Commands
Evidência: `cli.py`
- `bridge setup`: Menu TUI interativo.
- `bridge sync`:
  - `--mapping`: Filtra sync específico.
  - `--dry-run`: Simula sem upload.
  - `--workers`: Concorrência.
- `bridge status`: Checa conectividade e keys.
- `bridge version`: Exibe versão.

## Integrações
- **DataSnap API**: Envia dados para endpoints de ingestão (`/api/v1/schemas/{slug}/files`?).
- **Fontes Locais**: MySQL, PostgreSQL, Logs.
