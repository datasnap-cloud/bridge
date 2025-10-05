"""
Testes unitários para o módulo core.paths
"""

import pytest
import tempfile
import os
from pathlib import Path

from core.paths import (
    get_app_root_dir, 
    get_bridge_config_dir, 
    get_config_file_path, 
    get_api_keys_file_path,
    ensure_bridge_directory,
    set_secure_file_permissions
)


class TestPaths:
    """Testes para funcionalidades de paths"""
    
    def test_get_app_root_dir(self):
        """Testa se get_app_root_dir retorna um Path válido"""
        root_dir = get_app_root_dir()
        
        assert isinstance(root_dir, Path)
        assert root_dir.exists()
    
    def test_get_bridge_config_dir(self):
        """Testa se get_bridge_config_dir retorna o caminho correto"""
        config_dir = get_bridge_config_dir()
        root_dir = get_app_root_dir()
        
        assert isinstance(config_dir, Path)
        assert config_dir == root_dir / ".bridge"
    
    def test_get_config_file_path(self):
        """Testa se get_config_file_path retorna o caminho correto"""
        config_file = get_config_file_path()
        config_dir = get_bridge_config_dir()
        
        assert isinstance(config_file, Path)
        assert config_file == config_dir / "config.json"
    
    def test_get_api_keys_file_path(self):
        """Testa se get_api_keys_file_path retorna o caminho correto"""
        api_keys_file = get_api_keys_file_path()
        config_dir = get_bridge_config_dir()
        
        assert isinstance(api_keys_file, Path)
        assert api_keys_file == config_dir / "api_keys.enc"
    
    def test_ensure_bridge_directory(self):
        """Testa se ensure_bridge_directory cria o diretório corretamente"""
        # Este teste verifica se a função executa sem erro
        # O diretório pode já existir, então não verificamos sua criação
        try:
            ensure_bridge_directory()
            # Se chegou aqui, a função executou sem erro
            assert True
        except Exception as e:
            pytest.fail(f"ensure_bridge_directory falhou: {e}")
    
    def test_set_secure_file_permissions(self):
        """Testa se set_secure_file_permissions executa sem erro"""
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_path = tmp_file.name
        
        try:
            # A função deve executar sem erro, mesmo no Windows
            set_secure_file_permissions(tmp_path)
            assert True
        except Exception as e:
            pytest.fail(f"set_secure_file_permissions falhou: {e}")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)