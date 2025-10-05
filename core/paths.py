"""
Módulo para gerenciar caminhos e estrutura de diretórios do Bridge
"""

import os
import sys
from pathlib import Path
from typing import Union


def get_app_root_dir() -> Path:
    """
    Resolve o diretório base ao lado do app (não usar home do usuário).
    
    Returns:
        Path: Caminho para o diretório raiz da aplicação
    """
    if getattr(sys, 'frozen', False):
        # Se executando como executável empacotado (PyInstaller, etc.)
        app_dir = Path(sys.executable).parent
    else:
        # Se executando como script Python
        app_dir = Path(__file__).parent.parent
    
    return app_dir


def get_bridge_config_dir() -> Path:
    """
    Retorna o diretório de configuração .bridge/
    
    Returns:
        Path: Caminho para o diretório .bridge/
    """
    return get_app_root_dir() / ".bridge"


def get_config_file_path() -> Path:
    """
    Retorna o caminho para o arquivo config.json
    
    Returns:
        Path: Caminho para config.json
    """
    return get_bridge_config_dir() / "config.json"


def get_api_keys_file_path() -> Path:
    """
    Retorna o caminho para o arquivo api_keys.enc
    
    Returns:
        Path: Caminho para api_keys.enc
    """
    return get_bridge_config_dir() / "api_keys.enc"


def ensure_bridge_directory() -> None:
    """
    Cria o diretório .bridge/ se não existir, com permissões estritas
    """
    bridge_dir = get_bridge_config_dir()
    
    if not bridge_dir.exists():
        bridge_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        
        # Tentar definir permissões estritas (ignorar gracefully no Windows)
        try:
            os.chmod(bridge_dir, 0o700)
        except (OSError, AttributeError):
            # Windows ou sistema que não suporta chmod
            pass


def set_secure_file_permissions(file_path: Union[str, Path]) -> None:
    """
    Define permissões seguras para um arquivo (0o600)
    
    Args:
        file_path: Caminho para o arquivo
    """
    try:
        os.chmod(file_path, 0o600)
    except (OSError, AttributeError):
        # Windows ou sistema que não suporta chmod - ignorar gracefully
        pass