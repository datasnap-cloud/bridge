import unittest
from unittest.mock import patch, MagicMock
from core.crypto import get_machine_id, derive_key


class TestCrypto(unittest.TestCase):
    
    def test_get_machine_id(self):
        """Testa obtenção do machine ID"""
        machine_id = get_machine_id()
        
        # Deve retornar uma string não vazia
        assert isinstance(machine_id, str)
        assert len(machine_id) > 0
        
    def test_get_machine_id_consistent(self):
        """Testa que o machine ID é consistente"""
        id1 = get_machine_id()
        id2 = get_machine_id()
        
        # Deve ser sempre o mesmo
        assert id1 == id2
        
    def test_derive_key(self):
        """Testa derivação de chave"""
        machine_id = "test-machine-id"
        key = derive_key(machine_id)
        
        # Deve retornar 32 bytes
        assert isinstance(key, bytes)
        assert len(key) == 32
        
    def test_derive_key_different_salts(self):
        """Testa que machine IDs diferentes geram chaves diferentes"""
        key1 = derive_key("machine-1")
        key2 = derive_key("machine-2")
        
        # Chaves devem ser diferentes
        assert key1 != key2