"""
Testes unitários para o módulo core.secrets_store
"""

import pytest
import tempfile
import os
from datetime import datetime
from unittest.mock import patch, MagicMock

from core.secrets_store import APIKey, SecretsStore


class TestAPIKey:
    """Testes para a classe APIKey"""
    
    def test_api_key_creation(self):
        """Testa criação de APIKey"""
        api_key = APIKey(
            name="test-key",
            token="sk-1234567890abcdef",
            created_at=datetime.now()
        )
        
        assert api_key.name == "test-key"
        assert api_key.token == "sk-1234567890abcdef"
        assert isinstance(api_key.created_at, datetime)
    
    def test_mask_token(self):
        """Testa mascaramento de token"""
        api_key = APIKey(
            name="test-key",
            token="sk-1234567890abcdef",
            created_at="2024-01-15T10:30:45"
        )
        
        masked = api_key.get_masked_token()
        assert masked == "…cdef"
    
    def test_mask_token_short(self):
        """Testa mascaramento de token curto"""
        api_key = APIKey(
            name="test",
            token="short",
            created_at="2024-01-15T10:30:45Z"
        )
        masked = api_key.get_masked_token()
        
        # Token curto deve ser completamente mascarado
        assert masked == "…hort"
    
    def test_format_date(self):
        """Testa formatação de data"""
        api_key = APIKey(
            name="test-key",
            token="sk-1234567890abcdef",
            created_at="2024-01-15T10:30:45"
        )
        
        formatted = api_key.get_formatted_created_at()
        assert formatted == "2024-01-15 10:30"


class TestSecretsStore:
    """Testes para a classe SecretsStore"""
    
    def setup_method(self):
        """Setup para cada teste"""
        self.store = SecretsStore()
    
    @patch('core.secrets_store.get_api_keys_file_path')
    def test_load_nonexistent_file(self, mock_get_path):
        """Testa carregamento quando arquivo não existe"""
        mock_get_path.return_value = "/nonexistent/path"
        
        with patch('os.path.exists', return_value=False):
            self.store.load()
            
        assert self.store._loaded is True
        assert len(self.store._keys) == 0
    
    @patch('core.secrets_store.get_api_keys_file_path')
    @patch('core.secrets_store.decrypt_data')
    def test_load_existing_file(self, mock_decrypt, mock_get_path):
        """Testa carregamento de arquivo existente"""
        mock_get_path.return_value = "/existing/path"
        mock_decrypt.return_value = {
            "version": "1.0",
            "keys": [
                {
                    "name": "test-key",
                    "token": "sk-1234567890abcdef",
                    "created_at": "2024-01-15T10:30:45Z"
                }
            ]
        }
        
        with patch('os.path.exists', return_value=True):
            self.store.load()
        
        assert self.store._loaded is True
        assert len(self.store._keys) == 1
        assert self.store._keys[0].name == "test-key"
    
    def test_add_key(self):
        """Testa adição de chave"""
        # Simular que já foi carregado
        self.store._loaded = True
        
        with patch.object(self.store, 'save') as mock_save:
            # O método add_key não retorna valor, apenas adiciona
            self.store.add_key("test-key", "sk-1234567890abcdef")
        
        assert len(self.store._keys) == 1
        assert self.store._keys[0].name == "test-key"
        mock_save.assert_called_once()
    
    def test_add_duplicate_key(self):
        """Testa adição de chave duplicada"""
        self.store._loaded = True
        self.store._keys = [
            APIKey(name="existing-key", token="sk-existing", created_at="2024-01-15T10:30:45")
        ]
        
        # Deve lançar ValueError para chave duplicada
        with pytest.raises(ValueError, match="Já existe uma API Key com o nome 'existing-key'"):
            self.store.add_key("existing-key", "sk-new-token")
    
    def test_delete_key(self):
        """Testa exclusão de chave"""
        self.store._loaded = True
        self.store._keys = [
            APIKey(name="key-to-delete", token="sk-delete", created_at="2024-01-15T10:30:45")
        ]
        
        with patch.object(self.store, 'save') as mock_save:
            result = self.store.delete_key("key-to-delete")
        
        assert result is True
        assert len(self.store._keys) == 0
        mock_save.assert_called_once()
    
    def test_delete_nonexistent_key(self):
        """Testa exclusão de chave inexistente"""
        self.store._loaded = True
        self.store._keys = []
        
        result = self.store.delete_key("nonexistent-key")
        
        assert result is False
    
    def test_get_key_by_name(self):
        """Testa busca de chave por nome"""
        test_key = APIKey(name="find-me", token="sk-findme", created_at="2024-01-15T10:30:45")
        self.store._loaded = True
        self.store._keys = [test_key]
        
        found_key = self.store.get_key_by_name("find-me")
        
        assert found_key == test_key
    
    def test_get_nonexistent_key_by_name(self):
        """Testa busca de chave inexistente"""
        self.store._loaded = True
        self.store._keys = []
        
        found_key = self.store.get_key_by_name("nonexistent")
        
        assert found_key is None
    
    def test_list_keys(self):
        """Testa listagem de chaves"""
        key1 = APIKey(name="key1", token="sk-key1", created_at="2024-01-15T10:30:45")
        key2 = APIKey(name="key2", token="sk-key2", created_at="2024-01-15T10:30:45")
        
        self.store._loaded = True
        self.store._keys = [key1, key2]
        
        keys = self.store.list_keys()
        
        assert len(keys) == 2
        assert key1 in keys
        assert key2 in keys
    
    def test_get_keys_count(self):
        """Testa contagem de chaves"""
        self.store._loaded = True
        self.store._keys = [
            APIKey(name="key1", token="sk-key1", created_at="2024-01-15T10:30:45"),
            APIKey(name="key2", token="sk-key2", created_at="2024-01-15T10:30:45")
        ]
        
        count = self.store.get_keys_count()
        
        assert count == 2