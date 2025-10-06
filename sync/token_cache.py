"""
Módulo para cache de tokens de upload.
Gerencia o cache local de tokens para evitar requisições desnecessárias à API.
"""

import json
import time
from typing import Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass, asdict
import logging
from threading import Lock

from core.paths import BridgePaths


logger = logging.getLogger(__name__)


@dataclass
class CachedToken:
    """Representa um token em cache."""
    token_id: str
    upload_url: str
    schema_slug: str
    mapping_name: str
    expires_at: int
    created_at: int
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    def is_expired(self, buffer_seconds: int = 300) -> bool:
        """
        Verifica se o token está expirado.
        
        Args:
            buffer_seconds: Buffer de segurança em segundos
            
        Returns:
            True se o token estiver expirado
        """
        return time.time() >= (self.expires_at - buffer_seconds)
    
    def is_valid(self) -> bool:
        """
        Verifica se o token é válido.
        
        Returns:
            True se o token for válido
        """
        return not self.is_expired() and bool(self.token_id and self.upload_url)
    
    def time_until_expiry(self) -> int:
        """
        Retorna o tempo até a expiração em segundos.
        
        Returns:
            Segundos até a expiração (negativo se já expirado)
        """
        return int(self.expires_at - time.time())
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte o token para dicionário."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CachedToken':
        """Cria um token a partir de um dicionário."""
        return cls(**data)


class TokenCache:
    """Gerenciador de cache de tokens de upload."""
    
    def __init__(self, cache_dir: Path = None):
        """
        Inicializa o cache de tokens.
        
        Args:
            cache_dir: Diretório para armazenar o cache
        """
        self.paths = BridgePaths()
        self.cache_dir = cache_dir or self.paths.cache_dir
        self.cache_file = self.cache_dir / "upload_tokens.json"
        self._cache: Dict[str, CachedToken] = {}
        self._lock = Lock()
        
        # Garante que o diretório existe
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Carrega o cache existente
        self._load_cache()
    
    def _get_cache_key(self, schema_slug: str, mapping_name: str) -> str:
        """
        Gera uma chave única para o cache.
        
        Args:
            schema_slug: Slug do schema
            mapping_name: Nome do mapeamento
            
        Returns:
            Chave do cache
        """
        return f"{schema_slug}:{mapping_name}"
    
    def _load_cache(self) -> None:
        """Carrega o cache do arquivo."""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                for key, token_data in data.items():
                    try:
                        self._cache[key] = CachedToken.from_dict(token_data)
                    except Exception as e:
                        logger.warning(f"Erro ao carregar token {key}: {e}")
                
                logger.debug(f"Cache carregado: {len(self._cache)} tokens")
        except Exception as e:
            logger.error(f"Erro ao carregar cache: {e}")
            self._cache = {}
    
    def _save_cache(self) -> None:
        """Salva o cache no arquivo."""
        try:
            cache_data = {
                key: token.to_dict() 
                for key, token in self._cache.items()
            }
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
                
            logger.debug(f"Cache salvo: {len(self._cache)} tokens")
        except Exception as e:
            logger.error(f"Erro ao salvar cache: {e}")
    
    def get_token(self, schema_slug: str, mapping_name: str) -> Optional[CachedToken]:
        """
        Obtém um token do cache.
        
        Args:
            schema_slug: Slug do schema
            mapping_name: Nome do mapeamento
            
        Returns:
            Token em cache ou None se não encontrado/expirado
        """
        with self._lock:
            key = self._get_cache_key(schema_slug, mapping_name)
            token = self._cache.get(key)
            
            if token and token.is_valid():
                logger.debug(f"Token encontrado no cache: {key}")
                return token
            elif token:
                # Remove token expirado
                logger.debug(f"Token expirado removido: {key}")
                del self._cache[key]
                self._save_cache()
            
            return None
    
    def store_token(self, schema_slug: str, mapping_name: str, 
                   token_data: Dict[str, Any]) -> CachedToken:
        """
        Armazena um token no cache.
        
        Args:
            schema_slug: Slug do schema
            mapping_name: Nome do mapeamento
            token_data: Dados do token da API
            
        Returns:
            Token armazenado
        """
        with self._lock:
            # Extrai informações do token
            token_id = token_data.get('token_id') or token_data.get('id')
            upload_url = token_data.get('upload_url')
            expires_in = token_data.get('expires_in', 3600)  # 1 hora padrão
            
            if not token_id or not upload_url:
                raise ValueError("Token inválido: faltam token_id ou upload_url")
            
            # Calcula timestamp de expiração
            expires_at = int(time.time() + expires_in)
            
            # Cria o token em cache
            cached_token = CachedToken(
                token_id=token_id,
                upload_url=upload_url,
                schema_slug=schema_slug,
                mapping_name=mapping_name,
                expires_at=expires_at,
                created_at=int(time.time()),
                metadata=token_data.get('metadata', {})
            )
            
            # Armazena no cache
            key = self._get_cache_key(schema_slug, mapping_name)
            self._cache[key] = cached_token
            
            # Salva no arquivo
            self._save_cache()
            
            logger.info(f"Token armazenado no cache: {key} (expira em {expires_in}s)")
            return cached_token
    
    def invalidate_token(self, schema_slug: str, mapping_name: str) -> bool:
        """
        Invalida um token específico.
        
        Args:
            schema_slug: Slug do schema
            mapping_name: Nome do mapeamento
            
        Returns:
            True se o token foi removido
        """
        with self._lock:
            key = self._get_cache_key(schema_slug, mapping_name)
            if key in self._cache:
                del self._cache[key]
                self._save_cache()
                logger.debug(f"Token invalidado: {key}")
                return True
            return False
    
    def cleanup_expired(self) -> int:
        """
        Remove todos os tokens expirados do cache.
        
        Returns:
            Número de tokens removidos
        """
        with self._lock:
            expired_keys = [
                key for key, token in self._cache.items()
                if token.is_expired()
            ]
            
            for key in expired_keys:
                del self._cache[key]
            
            if expired_keys:
                self._save_cache()
                logger.info(f"Removidos {len(expired_keys)} tokens expirados")
            
            return len(expired_keys)
    
    def clear_cache(self) -> None:
        """Remove todos os tokens do cache."""
        with self._lock:
            self._cache.clear()
            self._save_cache()
            logger.info("Cache de tokens limpo")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Obtém estatísticas do cache.
        
        Returns:
            Estatísticas do cache
        """
        with self._lock:
            total_tokens = len(self._cache)
            valid_tokens = sum(1 for token in self._cache.values() if token.is_valid())
            expired_tokens = total_tokens - valid_tokens
            
            oldest_token = None
            newest_token = None
            
            if self._cache:
                tokens_by_age = sorted(self._cache.values(), key=lambda t: t.created_at)
                oldest_token = tokens_by_age[0].created_at
                newest_token = tokens_by_age[-1].created_at
            
            return {
                'total_tokens': total_tokens,
                'valid_tokens': valid_tokens,
                'expired_tokens': expired_tokens,
                'cache_file_size': self.cache_file.stat().st_size if self.cache_file.exists() else 0,
                'oldest_token_age': int(time.time() - oldest_token) if oldest_token else None,
                'newest_token_age': int(time.time() - newest_token) if newest_token else None
            }
    
    def list_tokens(self, include_expired: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        Lista todos os tokens no cache.
        
        Args:
            include_expired: Se deve incluir tokens expirados
            
        Returns:
            Dicionário com informações dos tokens
        """
        with self._lock:
            result = {}
            
            for key, token in self._cache.items():
                if not include_expired and token.is_expired():
                    continue
                
                result[key] = {
                    'token_id': token.token_id,
                    'schema_slug': token.schema_slug,
                    'mapping_name': token.mapping_name,
                    'created_at': token.created_at,
                    'expires_at': token.expires_at,
                    'time_until_expiry': token.time_until_expiry(),
                    'is_valid': token.is_valid(),
                    'is_expired': token.is_expired()
                }
            
            return result
    
    def get_token_info(self, schema_slug: str, mapping_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtém informações detalhadas de um token.
        
        Args:
            schema_slug: Slug do schema
            mapping_name: Nome do mapeamento
            
        Returns:
            Informações do token ou None se não encontrado
        """
        with self._lock:
            key = self._get_cache_key(schema_slug, mapping_name)
            token = self._cache.get(key)
            
            if not token:
                return None
            
            return {
                'token_id': token.token_id,
                'upload_url': token.upload_url,
                'schema_slug': token.schema_slug,
                'mapping_name': token.mapping_name,
                'created_at': token.created_at,
                'expires_at': token.expires_at,
                'time_until_expiry': token.time_until_expiry(),
                'is_valid': token.is_valid(),
                'is_expired': token.is_expired(),
                'metadata': token.metadata
            }


def create_token_cache(cache_dir: Path = None) -> TokenCache:
    """
    Cria uma instância do cache de tokens.
    
    Args:
        cache_dir: Diretório para o cache
        
    Returns:
        Instância do cache configurada
    """
    return TokenCache(cache_dir)