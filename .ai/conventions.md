Last verified: 2025-12-28
Source of truth: codebase scan

# Conventions

## CLI Style
- Typer para definição de comandos.
- Rich para output (cores, tabelas, progress bar).

## Estrutura
- `cli.py`: Apenas wiring de comandos.
- `sync/*.py`: Lógica de negócio.
