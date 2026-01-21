#!/usr/bin/env bash

# Garante que o script execute a partir do diretório onde ele está localizado
cd "$(dirname "$0")"

# Ativa o ambiente virtual Python
source venv/bin/activate

# Executa o comando de sincronização
python cli.py sync --all