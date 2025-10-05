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

from .logger import logger


def _generate_curl_command(method: str, url: str, headers: Dict[str, str], data: Optional[Dict[str, Any]] = None, params: Optional[Dict[str, Any]] = None) -> str:
    """
    Gera um comando curl equivalente à requisição HTTP
    
    Args:
        method: Método HTTP
        url: URL completa
        headers: Headers da requisição
        data: Dados JSON (opcional)
        params: Parâmetros de query (opcional)
        
    Returns:
        str: Comando curl formatado
    """
    # Construir URL com parâmetros se existirem
    if params:
        param_str = "&".join([f"{k}={v}" for k, v in params.items()])
        url = f"{url}?{param_str}"
    
    # Começar comando curl
    curl_parts = [f"curl -X {method}"]
    
    # Adicionar headers
    for key, value in headers.items():
        # Mascarar token de autorização para segurança
        if key.lower() == 'authorization' and 'Bearer' in value:
            masked_value = f"Bearer {value.split(' ')[1][:10]}..."
            curl_parts.append(f'-H "{key}: {masked_value}"')
        else:
            curl_parts.append(f'-H "{key}: {value}"')
    
    # Adicionar dados JSON se existirem
    if data:
        json_data = json.dumps(data, separators=(',', ':'))
        curl_parts.append(f"-d '{json_data}'")
    
    # Adicionar URL
    curl_parts.append(f'"{url}"')
    
    return " \\\n  ".join(curl_parts)


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
        
        logger.debug(f"🌐 Inicializando DataSnapHTTPClient com base_url: {self.base_url}")
        self.session = self._create_session()
        logger.debug("✅ Cliente HTTP inicializado com sucesso")
    
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
        
        # Configurar SSL - fallback para Windows
        try:
            import certifi
            # Testar se certifi funciona com um site simples
            test_session = requests.Session()
            test_session.verify = certifi.where()
            test_response = test_session.get('https://httpbin.org/get', timeout=3)
            
            # Se chegou aqui, certifi funciona
            session.verify = certifi.where()
            logger.debug("🔒 SSL: Usando certificados do certifi")
        except Exception as e:
            # Fallback: desabilitar verificação SSL com aviso
            session.verify = False
            logger.warning(f"⚠️  SSL: Verificação desabilitada devido a problemas de certificado: {e}")
            # Suprimir warnings de SSL
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
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
        headers = {
            'User-Agent': 'insomnia/11.1.0',  # Usar o mesmo User-Agent que funciona
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        if token:
            headers['Authorization'] = f'Bearer {token}'
        
        # Gerar e logar comando curl equivalente
        curl_command = _generate_curl_command(method, url, headers, data, params)
        logger.debug(f"🔧 Comando curl equivalente:\n{curl_command}")
        
        # Log da requisição
        logger.debug(f"🔄 {method} {url}")
        if params:
            logger.debug(f"📋 Query params: {params}")
        if data:
            logger.debug(f"📤 Request data: {json.dumps(data, indent=2)}")
        if token:
            logger.debug(f"🔑 Token presente: {token[:10]}...")
        
        # Configurar timeouts
        timeout = (10, 20)  # (connect_timeout, read_timeout)
        
        try:
            start_time = time.time()
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                params=params,
                timeout=timeout
                # verify é configurado na sessão
            )
            elapsed_time = time.time() - start_time
            
            logger.debug(f"⏱️ Requisição completada em {elapsed_time:.2f}s - Status: {response.status_code}")
            
            # Tentar parsear JSON da resposta
            try:
                response_data = response.json()
                logger.debug(f"📥 Response data: {json.dumps(response_data, indent=2)}")
            except (json.JSONDecodeError, ValueError):
                # Se não for JSON válido, usar texto da resposta
                response_data = {"message": response.text or "Resposta vazia"}
                logger.debug(f"📥 Response text: {response.text}")
            
            return response.status_code, response_data
            
        except requests.exceptions.Timeout:
            logger.error("⏰ Timeout na requisição HTTP")
            logger.error(f"🔧 Comando curl que falhou:\n{curl_command}")
            raise requests.RequestException("Timeout na requisição")
        except requests.exceptions.SSLError as e:
            logger.warning(f"🔒 Erro SSL detectado: {e}")
            logger.warning("🔄 Tentando novamente sem verificação SSL...")
            
            # Recriar sessão sem SSL
            self.session.verify = False
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=data,
                    params=params,
                    timeout=timeout
                )
                elapsed_time = time.time() - start_time
                
                logger.debug(f"⏱️ Requisição completada em {elapsed_time:.2f}s - Status: {response.status_code}")
                
                # Tentar parsear JSON da resposta
                try:
                    response_data = response.json()
                    logger.debug(f"📥 Response data: {json.dumps(response_data, indent=2)}")
                except (json.JSONDecodeError, ValueError):
                    # Se não for JSON válido, usar texto da resposta
                    response_data = {"message": response.text or "Resposta vazia"}
                    logger.debug(f"📥 Response text: {response.text}")
                
                return response.status_code, response_data
                
            except Exception as retry_e:
                logger.error(f"❌ Falha mesmo sem SSL: {retry_e}")
                raise requests.RequestException(f"Erro SSL e falha no retry: {retry_e}")
        except requests.exceptions.ConnectionError as e:
            # Verificar se é um erro SSL mascarado como ConnectionError
            if "CERTIFICATE_VERIFY_FAILED" in str(e) or "certificate verify failed" in str(e).lower():
                logger.warning(f"🔒 Erro SSL mascarado como ConnectionError: {e}")
                logger.warning("🔄 Tentando novamente sem verificação SSL...")
                
                # Recriar sessão sem SSL
                self.session.verify = False
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                
                try:
                    response = self.session.request(
                        method=method,
                        url=url,
                        headers=headers,
                        json=data,
                        params=params,
                        timeout=timeout
                    )
                    elapsed_time = time.time() - start_time
                    
                    logger.debug(f"⏱️ Requisição completada em {elapsed_time:.2f}s - Status: {response.status_code}")
                    
                    # Tentar parsear JSON da resposta
                    try:
                        response_data = response.json()
                        logger.debug(f"📥 Response data: {json.dumps(response_data, indent=2)}")
                    except (json.JSONDecodeError, ValueError):
                        # Se não for JSON válido, usar texto da resposta
                        response_data = {"message": response.text or "Resposta vazia"}
                        logger.debug(f"📥 Response text: {response.text}")
                    
                    return response.status_code, response_data
                    
                except Exception as retry_e:
                    logger.error(f"❌ Falha mesmo sem SSL: {retry_e}")
                    raise requests.RequestException(f"Erro SSL e falha no retry: {retry_e}")
            else:
                logger.error("🔌 Erro de conexão HTTP")
                logger.error(f"🔧 Comando curl que falhou:\n{curl_command}")
                raise requests.RequestException("Erro de conexão")
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na requisição HTTP: {e}")
            logger.error(f"🔧 Comando curl que falhou:\n{curl_command}")
            raise requests.RequestException(f"Erro na requisição: {e}")
    
    def validate_token(self, token: str) -> Tuple[bool, str]:
        """
        Valida um token de API fazendo uma requisição para /auth/me
        
        Args:
            token: Token para validar
            
        Returns:
            Tuple[bool, str]: (is_valid, message)
        """
        logger.debug(f"🔍 Validando token: {token[:10]}...")
        
        try:
            status_code, response_data = self._make_request(
                method="GET",
                endpoint="auth/me",
                token=token
            )
            
            if status_code == 200:
                logger.debug("✅ Token validado com sucesso")
                return True, "Token válido"
            elif status_code == 401:
                logger.warning("🚫 Token inválido ou expirado")
                return False, "Token inválido ou expirado"
            elif status_code == 403:
                logger.warning("🔒 Token sem permissões necessárias")
                return False, "Token sem permissões necessárias"
            else:
                message = response_data.get("message", f"Erro HTTP {status_code}")
                logger.error(f"❌ Erro na validação do token - Status {status_code}: {message}")
                return False, f"Erro na validação: {message}"
                
        except requests.RequestException as e:
            logger.error(f"🌐 Erro de rede na validação do token: {e}")
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
        Testa a conectividade com a API usando um endpoint válido
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            # Usar um endpoint que sabemos que existe
            status_code, response_data = self._make_request(
                method="GET",
                endpoint="auth/me",  # Endpoint que sabemos que existe
                token="invalid_token_for_test"  # Token inválido apenas para testar conectividade
            )
            
            # Se chegou até aqui, a conectividade está OK
            # Status 401 é esperado com token inválido
            if status_code in [200, 401]:
                return True, "Conexão OK"
            else:
                return False, f"API retornou status {status_code}"
                
        except requests.RequestException as e:
            return False, f"Erro de rede: {e}"


# Instância global do cliente HTTP
http_client = DataSnapHTTPClient()