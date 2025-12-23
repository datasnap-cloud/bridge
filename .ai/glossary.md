# Glossary - DataSnap Bridge

## Termos Específicos
- **Bridge**: A própria ferramenta CLI.
- **Mapping**: Configuração que define DE onde ler (Source: Table X) e PARA onde enviar (Schema Slug Y).
- **JsonL**: Formato de arquivo onde cada linha é um JSON válido. Padrão de troca de dados DataSnap.
- **Upload Token**: URL assinada (S3 Presigned URL ou OCI PAR) temporária que permite enviar o arquivo batch.
- **Incremental Sync**: Sincronização que lê apenas o que mudou desde a última execução (baseado em coluna `updated_at` ou `id`).
- **Full Sync**: Sincronização que lê a tabela inteira e substitui (ou faz merge, dependendo da estratégia).
- **Secrets Store**: Arquivo criptografado local (`.bridge/secrets.json`) contendo credenciais de banco e da API.
