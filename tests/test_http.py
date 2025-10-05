import unittest
from unittest.mock import patch, MagicMock
from core.http import DataSnapHTTPClient, http_client


class TestDataSnapHTTPClient(unittest.TestCase):
    
    def setUp(self):
        """Configuração inicial para cada teste"""
        self.client = DataSnapHTTPClient()
    
    def test_client_initialization(self):
        """Testa inicialização do cliente"""
        assert self.client.base_url == "https://api.datasnap.cloud/"
        assert self.client.session is not None
        
    def test_global_client_instance(self):
        """Testa instância global do cliente"""
        assert http_client is not None
        assert isinstance(http_client, DataSnapHTTPClient)
        
    @patch('core.http.DataSnapHTTPClient._make_request')
    def test_validate_token_success(self, mock_request):
        """Testa validação de token com sucesso"""
        mock_request.return_value = (200, {"user": "test"})
        
        success, message = self.client.validate_token("valid-token")
        
        assert success is True
        assert message == "Token válido"
        
    @patch('core.http.DataSnapHTTPClient._make_request')
    def test_get_schemas_success(self, mock_request):
        """Testa busca de schemas com sucesso"""
        mock_request.return_value = (200, {"schemas": []})
        
        success, data = self.client.get_schemas("valid-token")
        
        assert success is True
        assert data == {"schemas": []}
        
    @patch('core.http.DataSnapHTTPClient._make_request')
    def test_test_connection_success(self, mock_request):
        """Testa conexão com sucesso"""
        mock_request.return_value = (200, {"status": "ok"})
        
        success, message = self.client.test_connection()
        
        assert success is True
        assert message == "Conexão OK"