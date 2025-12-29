Last verified: 2025-12-28
Source of truth: codebase scan

# Constraints & Limits

## Segurança
- **Segredos**: Armazenados localmente mas criptografados.
- **SSL**: Validação de certificados mandatória (certifi).

## Performance
- **Sync**: Paralelismo controlado por thread types (asyncio).
- **Batch Size**: Default 10000 linhas.
- **Memória**: Cuidado com extração de logs grandes (usa `max_memory_mb` para streaming).
