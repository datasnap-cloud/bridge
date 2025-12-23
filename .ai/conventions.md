# Conventions - DataSnap Bridge

## Code Style
- **Python**: PEP 8.
- **Typing**: Type hints obrigatórios em novas funções (`def func(a: int) -> str:`).
- **Docstrings**: Google Style ou NumPy Style.

## Architecture
- **Sync Runner**: Padrão Producer-Consumer (Extractor produz batches, Uploader consome).
- **State**: O estado da sincronização (cursor incremental) é salvo localmente em SQLite ou JSON (`mapping_state_store.py`). Não confiar apenas na API remota para saber onde parou.

## Error Handling
- **Graceful Degradation**: Falha em um mapeamento não deve parar os outros (`--all`).
- **User Feedback**: Usar `rich` para mostrar progresso e erros amigáveis. Logs detalhados em arquivo (`bridge.log`), apenas info/erro crucial no terminal.

## Versioning
- `cli.py` define a versão.
- Atualizações devem ser compatíveis com versões antigas da API Backend (Backwards Compatibility).
