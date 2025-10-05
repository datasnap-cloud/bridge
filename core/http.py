"""
Módulo HTTP para comunicação com a API DataSnap
"""

import os
import time
import json
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class DataSnapHTTPClient:
    """Cliente HTTP para a API DataSnap com retries e configurações otimizadas"""
    
    def __init__(self, base_url: Optional[str] = None):
        """
        Inicializa o cliente HTTP
        
        Args:
            base_url: URL base da API (padrão: https://api.datasnap.cloud)
        """
        self.base_url = base_url or os.getenv("DATASNAP_API_BASE_URL", "https://api.datasnap.cloud")
        if not self.base_url.endswith('/'):
            self.base_url += '/'
        
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """
        Cria uma sessão HTTP configurada com retries e timeouts
        
        Returns:
            requests.Session: Sessão configurada
        """
        session = requests.Session()
        
        # Configurar headers padrão
        session.headers.update({
            'User-Agent': 'BridgeSetup/0.1 (+datasnap)',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Configurar estratégia de retry
        retry_strategy = Retry(
            total=3,  # 3 tentativas
            backoff_factor=1,  # Backoff exponencial: 1s, 2s, 4s
            status_forcelist=[429, 500, 502, 503, 504],  # Status codes para retry
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE"]
        )
        
        # Aplicar adapter com retry para HTTP e HTTPS
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        token: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Faz uma requisição HTTP
        
        Args:
            method: Método HTTP (GET, POST, etc.)
            endpoint: Endpoint da API (ex: 'auth/me')
            token: Token de autorização (Bearer)
            data: Dados para enviar no body (JSON)
            params: Parâmetros de query string
            
        Returns:
            Tuple[int, Dict[str, Any]]: (status_code, response_data)
            
        Raises:
            requests.RequestException: Para erros de rede/HTTP
        """
        url = urljoin(self.base_url, endpoint)
        
        # Configurar headers da requisição
        headers = {}
        if token:
            headers['Authorization'] = f'Bearer {token}'
        
        # Configurar timeouts
        timeout = (10, 20)  # (connect_timeout, read_timeout)
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                params=params,
                timeout=timeout
            )
            
            # Tentar parsear JSON da resposta
            try:
                response_data = response.json()
            except (json.JSONDecodeError, ValueError):
                # Se não for JSON válido, usar texto da resposta
                response_data = {"message": response.text or "Resposta vazia"}
            
            return response.status_code, response_data
            
        except requests.exceptions.Timeout:
            raise requests.RequestException("Timeout na requisição")
        except requests.exceptions.ConnectionError:
            raise requests.RequestException("Erro de conexão")
        except requests.exceptions.RequestException as e:
            raise requests.RequestException(f"Erro na requisição: {e}")
    
    def validate_token(self, token: str) -> Tuple[bool, str]:
        """
        Valida um token de API fazendo uma requisição para /auth/me
        
        Args:
            token: Token para validar
            
        Returns:
            Tuple[bool, str]: (is_valid, message)
        """
        try:
            status_code, response_data = self._make_request(
                method="GET",
                endpoint="auth/me",
                token=token
            )
            
            if status_code == 200:
                return True, "Token válido"
            elif status_code == 401:
                return False, "Token inválido ou expirado"
            elif status_code == 403:
                return False, "Token sem permissões necessárias"
            else:
                message = response_data.get("message", f"Erro HTTP {status_code}")
                return False, f"Erro na validação: {message}"
                
        except requests.RequestException as e:
            return False, f"Erro de rede: {e}"
    
    def get_schemas(self, token: str) -> Tuple[bool, Any]:
        """
        Busca os schemas/modelos de dados da API
        
        Args:
            token: Token de autorização
            
        Returns:
            Tuple[bool, Any]: (success, data_or_error_message)
        """
        try:
            status_code, response_data = self._make_request(
                method="GET",
                endpoint="v1/schemas",
                token=token
            )
            
            if status_code == 200:
                return True, response_data
            elif status_code == 401:
                return False, "Token inválido ou expirado"
            elif status_code == 403:
                return False, "Token sem permissões para acessar schemas"
            elif status_code == 404:
                return False, "Endpoint de schemas não encontrado"
            else:
                message = response_data.get("message", f"Erro HTTP {status_code}")
                return False, f"Erro ao buscar schemas: {message}"
                
        except requests.RequestException as e:
            return False, f"Erro de rede: {e}"
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Testa a conectividade com a API
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            status_code, response_data = self._make_request(
                method="GET",
                endpoint="health",  # Assumindo que existe um endpoint de health
            )
            
            if status_code == 200:
                return True, "Conexão OK"
            else:
                return False, f"API retornou status {status_code}"
                
        except requests.RequestException as e:
            return False, f"Erro de conexão: {e}"


# Instância global do cliente HTTP
http_client = DataSnapHTTPClient()