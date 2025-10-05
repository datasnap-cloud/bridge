"""
Gerenciamento de API Keys criptografadas
"""

import json
import os
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, asdict

from core.crypto import encrypt_data, decrypt_data
from core.paths import get_api_keys_file_path


@dataclass
class APIKey:
    """Representa uma API Key armazenada"""
    name: str
    token: str
    created_at: str
    
    def get_masked_token(self) -> str:
        """
        Retorna o token mascarado (apenas últimos 4 caracteres)
        
        Returns:
            str: Token mascarado no formato "…XXXX"
        """
        if len(self.token) <= 4:
            return "…" + self.token
        return "…" + self.token[-4:]
    
    def get_formatted_created_at(self) -> str:
        """
        Retorna a data de criação formatada
        
        Returns:
            str: Data formatada no formato "YYYY-MM-DD HH:MM"
        """
        try:
            dt = datetime.fromisoformat(self.created_at.replace('Z', '+00:00'))
            return dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            return self.created_at


class SecretsStore:
    """Gerenciador de API Keys criptografadas"""
    
    def __init__(self):
        self._keys: List[APIKey] = []
        self._loaded = False
    
    def load(self) -> None:
        """
        Carrega as API Keys do arquivo criptografado
        """
        try:
            api_keys_path = get_api_keys_file_path()
            
            if not os.path.exists(api_keys_path):
                self._keys = []
                self._loaded = True
                return
            
            # Descriptografar e carregar
            decrypted_data = decrypt_data(api_keys_path)
            
            if not decrypted_data or not isinstance(decrypted_data, dict):
                self._keys = []
                self._loaded = True
                return
            
            # Validar estrutura
            if "version" not in decrypted_data or "keys" not in decrypted_data:
                self._keys = []
                self._loaded = True
                return
            
            # Carregar keys
            self._keys = []
            for key_data in decrypted_data.get("keys", []):
                if all(field in key_data for field in ["name", "token", "created_at"]):
                    self._keys.append(APIKey(**key_data))
            
            self._loaded = True
            
        except Exception as e:
            # Em caso de erro, inicializar vazio
            self._keys = []
            self._loaded = True
    
    def save(self) -> None:
        """
        Salva as API Keys no arquivo criptografado
        """
        try:
            # Preparar dados para salvar
            data = {
                "version": 1,
                "keys": [asdict(key) for key in self._keys]
            }
            
            # Criptografar e salvar
            api_keys_path = get_api_keys_file_path()
            encrypt_data(data, api_keys_path)
            
        except Exception as e:
            raise Exception(f"Erro ao salvar API Keys: {e}")
    
    def add_key(self, name: str, token: str) -> None:
        """
        Adiciona uma nova API Key
        
        Args:
            name: Nome da API Key
            token: Token da API Key
            
        Raises:
            ValueError: Se já existe uma key com o mesmo nome
        """
        if not self._loaded:
            self.load()
        
        # Verificar se já existe
        if any(key.name == name for key in self._keys):
            raise ValueError(f"Já existe uma API Key com o nome '{name}'")
        
        # Adicionar nova key
        new_key = APIKey(
            name=name,
            token=token,
            created_at=datetime.utcnow().isoformat() + "Z"
        )
        
        self._keys.append(new_key)
        self.save()
    
    def delete_key(self, name: str) -> bool:
        """
        Remove uma API Key pelo nome
        
        Args:
            name: Nome da API Key a ser removida
            
        Returns:
            bool: True se removeu com sucesso, False se não encontrou
        """
        if not self._loaded:
            self.load()
        
        # Encontrar e remover
        for i, key in enumerate(self._keys):
            if key.name == name:
                del self._keys[i]
                self.save()
                return True
        
        return False
    
    def list_keys(self) -> List[APIKey]:
        """
        Lista todas as API Keys
        
        Returns:
            List[APIKey]: Lista de API Keys
        """
        if not self._loaded:
            self.load()
        
        return self._keys.copy()
    
    def get_key_by_name(self, name: str) -> Optional[APIKey]:
        """
        Busca uma API Key pelo nome
        
        Args:
            name: Nome da API Key
            
        Returns:
            Optional[APIKey]: API Key encontrada ou None
        """
        if not self._loaded:
            self.load()
        
        for key in self._keys:
            if key.name == name:
                return key
        
        return None
    
    def get_keys_count(self) -> int:
        """
        Retorna o número de API Keys cadastradas
        
        Returns:
            int: Número de API Keys
        """
        if not self._loaded:
            self.load()
        
        return len(self._keys)


# Instância global do secrets store
secrets_store = SecretsStore()