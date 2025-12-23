# Constraints - DataSnap Bridge

## Performance & Recursos
- **Memória**: O Extractor deve respeitar limites de memória (`--max-mb`). Não carregar tudo em RAM. Usar `yield` e processamento em streaming (batches).
- **Concurrency**: Uploads devem ser paralelos (`asyncio` / `ThreadPoolExecutor`), mas extração de banco geralmente é sequencial por conexão.
- **Timeouts**: A rede do cliente pode ser instável. O `uploader.py` deve implementar retries exponenciais.

## Segurança
- **Segredos**: NUNCA salvar API Keys ou senhas de banco em texto plano. Usar sempre `secrets_store.py` (Fernet encryption).
- **Logs**: Não logar conteúdo sensível dos dados extraídos (PII).

## Dependências
- **Python**: Compatibilidade mínima Python 3.9+.
- **Libs**: Manter `requirements.txt` enxuto. Evitar dependências pesadas de Data Science (Pandas) se possível, para facilitar instalação em ambientes restritos.
- **Drivers**: `psycopg2-binary` e `pymysql` são os drivers padrão.

## Deploy
- A ferramenta deve ser capaz de rodar sem root.
- O diretório `.bridge` armazena estado e segredos, deve ter permissão de escrita.
