"""
Módulo para comunicação com a API do DataSnap.
Gerencia autenticação, upload de arquivos e obtenção de tokens.
"""

import requests
import json
import time
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
import logging
from urllib.parse import urljoin

from core.paths import get_api_keys_file_path
from core.logger import logger


class DataSnapAPIError(Exception):
    """Exceção personalizada para erros da API do DataSnap."""
    pass


class DataSnapAPI:
    """Cliente para comunicação com a API do DataSnap."""
    
    def __init__(self, base_url: str = None):
        """
        Inicializa o cliente da API.
        
        Args:
            base_url: URL base da API. Se não fornecida, usa a padrão.
        """
        self.base_url = base_url or "https://api.datasnap.cloud"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DataSnap-Bridge/1.0',
            'Content-Type': 'application/json'
        })
        
        # Desabilitar verificação SSL
        self.session.verify = False
        # Suprimir warnings de SSL
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        self._api_key = None
        self._load_api_key()
    
    def _load_api_key(self) -> None:
        """Carrega a chave da API do arquivo de configuração."""
        try:
            # Usar o secrets_store para carregar as chaves criptografadas
            from core.secrets_store import secrets_store
            
            secrets_store.load()
            keys = secrets_store.list_keys()
            
            if keys:
                # Usar a primeira chave disponível
                first_key = keys[0]
                self._api_key = first_key.token
                self.session.headers['Authorization'] = f'Bearer {self._api_key}'
                logger.debug(f"API Key carregada: {first_key.name}")
                return
            else:
                logger.warning("Nenhuma API Key encontrada no arquivo criptografado")
                
        except Exception as e:
            logger.warning(f"Erro ao carregar chave da API: {e}")
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Faz uma requisição HTTP para a API.
        
        Args:
            method: Método HTTP (GET, POST, etc.)
            endpoint: Endpoint da API
            **kwargs: Argumentos adicionais para requests
            
        Returns:
            Resposta da requisição
            
        Raises:
            DataSnapAPIError: Em caso de erro na API
        """
        # Adiciona prefixo v1/ para todos os endpoints exceto /me, /health e /status
        endpoint = endpoint.lstrip('/')
        if not endpoint.startswith(('me', 'health', 'status')):
            endpoint = f'v1/{endpoint}'
        
        url = urljoin(self.base_url, endpoint)
        
        try:
            response = self.session.request(method, url, **kwargs)
            
            # Log da requisição
            logger.debug(f"{method} {url} - Status: {response.status_code}")
            
            if response.status_code >= 400:
                error_msg = f"Erro na API: {response.status_code}"
                try:
                    error_data = response.json()
                    if 'message' in error_data:
                        error_msg += f" - {error_data['message']}"
                except:
                    error_msg += f" - {response.text}"
                
                raise DataSnapAPIError(error_msg)
            
            return response
            
        except requests.RequestException as e:
            raise DataSnapAPIError(f"Erro de conexão: {e}")
    
    def test_connection(self) -> bool:
        """
        Testa a conexão com a API.
        
        Returns:
            True se a conexão estiver funcionando
        """
        try:
            response = self._make_request('GET', '/health')
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Erro ao testar conexão: {e}")
            return False
    
    def get_upload_token(self, schema_slug: str, mapping_name: str) -> Dict[str, Any]:
        """
        Obtém um token de upload para um schema específico.
        
        Args:
            schema_slug: Slug do schema
            mapping_name: Nome do mapeamento
            
        Returns:
            Dados do token de upload
            
        Raises:
            DataSnapAPIError: Em caso de erro na API
        """
        endpoint = f'/schemas/{schema_slug}/generate-upload-token'
        url = urljoin(self.base_url, endpoint)
        data = {
            'mapping_name': mapping_name,
            'timestamp': int(time.time()),
            'minutes': 30
        }
        
        logger.info(f"🔄 Tentativa de obter token de upload")
        logger.info(f"📍 URL montada: {url}")
        logger.info(f"📦 Corpo da requisição JSON: {json.dumps(data, indent=2)}")
        logger.debug(f"📦 Dados da requisição: {data}")
        
        response = self._make_request('POST', endpoint, json=data)
        
        # Log da resposta
        logger.info(f"📊 Status da resposta: {response.status_code}")
        
        if response.status_code == 200:
            response_data = response.json()
            token = response_data.get('token', '')
            truncated_token = token[:20] + '...' if len(token) > 20 else token
            logger.info(f"✅ Token obtido com sucesso (truncado): {truncated_token}")
            logger.debug(f"📋 Resposta completa: {response_data}")
            return response_data
        else:
            logger.error(f"❌ Falha ao obter token de upload")
            logger.error(f"📋 Resposta recebida: {response.text}")
            response.raise_for_status()
            return response.json()
    
    def upload_file(self, upload_url: str, file_path: Path, 
                   content_type: str = 'application/x-ndjson') -> bool:
        """
        Faz upload de um arquivo usando URL pré-assinada.
        
        Args:
            upload_url: URL pré-assinada para upload
            file_path: Caminho do arquivo a ser enviado
            content_type: Tipo de conteúdo do arquivo
            
        Returns:
            True se o upload foi bem-sucedido
            
        Raises:
            DataSnapAPIError: Em caso de erro no upload
        """
        if not file_path.exists():
            raise DataSnapAPIError(f"Arquivo não encontrado: {file_path}")
        
        try:
            with open(file_path, 'rb') as f:
                headers = {'Content-Type': content_type}
                response = requests.put(upload_url, data=f, headers=headers)
                
            if response.status_code not in [200, 201, 204]:
                raise DataSnapAPIError(f"Erro no upload: {response.status_code}")
            
            logger.info(f"Upload concluído: {file_path.name}")
            return True
            
        except requests.RequestException as e:
            raise DataSnapAPIError(f"Erro no upload: {e}")
    
    def get_schema_info(self, schema_slug: str) -> Dict[str, Any]:
        """
        Obtém informações sobre um schema.
        
        Args:
            schema_slug: Slug do schema
            
        Returns:
            Informações do schema
            
        Raises:
            DataSnapAPIError: Em caso de erro na API
        """
        endpoint = f'/schemas/{schema_slug}'
        response = self._make_request('GET', endpoint)
        return response.json()
    
    def list_schemas(self) -> List[Dict[str, Any]]:
        """
        Lista todos os schemas disponíveis.
        
        Returns:
            Lista de schemas
            
        Raises:
            DataSnapAPIError: Em caso de erro na API
        """
        endpoint = '/schemas'
        response = self._make_request('GET', endpoint)
        return response.json()
    
    def get_upload_history(self, schema_slug: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Obtém o histórico de uploads de um schema.
        
        Args:
            schema_slug: Slug do schema
            limit: Número máximo de registros
            
        Returns:
            Lista de uploads
            
        Raises:
            DataSnapAPIError: Em caso de erro na API
        """
        endpoint = f'/schemas/{schema_slug}/uploads'
        params = {'limit': limit}
        response = self._make_request('GET', endpoint, params=params)
        return response.json()
    
    def validate_token(self, token_id: str) -> Dict[str, Any]:
        """
        Valida um token de upload.
        
        Args:
            token_id: ID do token
            
        Returns:
            Status do token
            
        Raises:
            DataSnapAPIError: Em caso de erro na API
        """
        endpoint = f'/upload-tokens/{token_id}/validate'
        response = self._make_request('GET', endpoint)
        return response.json()
    
    def get_api_status(self) -> Dict[str, Any]:
        """
        Obtém o status da API.
        
        Returns:
            Status da API
            
        Raises:
            DataSnapAPIError: Em caso de erro na API
        """
        endpoint = '/status'
        response = self._make_request('GET', endpoint)
        return response.json()
    
    def refresh_api_key(self) -> None:
        """Recarrega a chave da API do arquivo de configuração."""
        self._load_api_key()
    
    def set_api_key(self, api_key: str) -> None:
        """
        Define a chave da API programaticamente.
        
        Args:
            api_key: Chave da API
        """
        self._api_key = api_key
        self.session.headers['Authorization'] = f'Bearer {api_key}'


def create_api_client(base_url: str = None) -> DataSnapAPI:
    """
    Cria uma instância do cliente da API.
    
    Args:
        base_url: URL base da API
        
    Returns:
        Cliente da API configurado
    """
    return DataSnapAPI(base_url)


def test_api_connection(base_url: str = None) -> Tuple[bool, str]:
    """
    Testa a conexão com a API do DataSnap.
    
    Args:
        base_url: URL base da API
        
    Returns:
        Tupla (sucesso, mensagem)
    """
    try:
        client = create_api_client(base_url)
        if client.test_connection():
            return True, "Conexão com a API estabelecida com sucesso"
        else:
            return False, "Falha ao conectar com a API"
    except Exception as e:
        return False, f"Erro ao testar conexão: {e}"