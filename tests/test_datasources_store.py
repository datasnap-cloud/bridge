"""
Testes para o módulo datasources_store
"""

import unittest
from unittest.mock import patch, MagicMock
import json
import os
from datetime import datetime

from core.datasources_store import (
    DatabaseConnection, TableSelection, DataSource, DataSourcesStore
)


class TestDatabaseConnection(unittest.TestCase):
    """Testes para a classe DatabaseConnection"""
    
    def test_database_connection_creation(self):
        """Testa criação de DatabaseConnection"""
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            database="testdb",
            user="testuser",
            password="testpass",
            options={"charset": "utf8mb4"}
        )
        
        self.assertEqual(conn.host, "localhost")
        self.assertEqual(conn.port, 3306)
        self.assertEqual(conn.database, "testdb")
        self.assertEqual(conn.user, "testuser")
        self.assertEqual(conn.password, "testpass")
        self.assertEqual(conn.options, {"charset": "utf8mb4"})


class TestTableSelection(unittest.TestCase):
    """Testes para a classe TableSelection"""
    
    def test_table_selection_creation(self):
        """Testa criação de TableSelection"""
        tables = TableSelection(
            selected=["users", "orders"],
            last_discovery_at="2024-01-15T10:30:45Z"
        )
        
        self.assertEqual(tables.selected, ["users", "orders"])
        self.assertEqual(tables.last_discovery_at, "2024-01-15T10:30:45Z")
    
    def test_table_selection_empty(self):
        """Testa criação de TableSelection vazia"""
        tables = TableSelection(selected=[])
        
        self.assertEqual(tables.selected, [])
        self.assertIsNone(tables.last_discovery_at)


class TestDataSource(unittest.TestCase):
    """Testes para a classe DataSource"""
    
    def test_datasource_creation(self):
        """Testa criação de DataSource"""
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            database="testdb",
            user="testuser",
            password="testpass"
        )
        
        tables = TableSelection(selected=[])
        
        datasource = DataSource(
            id="test-id",
            type="mysql",
            name="test-source",
            created_at="2024-01-15T10:30:45Z",
            conn=conn,
            tables=tables
        )
        
        self.assertEqual(datasource.id, "test-id")
        self.assertEqual(datasource.type, "mysql")
        self.assertEqual(datasource.name, "test-source")
        self.assertEqual(datasource.created_at, "2024-01-15T10:30:45Z")
        self.assertEqual(datasource.conn, conn)
        self.assertIsInstance(datasource.tables, TableSelection)
    
    def test_get_connection_summary(self):
        """Testa resumo da conexão"""
        conn = DatabaseConnection(
            host="db.example.com",
            port=3306,
            database="production",
            user="admin",
            password="secret"
        )
        
        datasource = DataSource(
            id="test-id",
            type="mysql",
            name="test-source",
            created_at="2024-01-15T10:30:45Z",
            conn=conn
        )
        
        summary = datasource.get_connection_summary()
        self.assertEqual(summary, "db.example.com:3306/production")
    
    def test_get_formatted_created_at(self):
        """Testa formatação da data de criação"""
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            database="testdb",
            user="testuser",
            password="testpass"
        )
        
        datasource = DataSource(
            id="test-id",
            type="mysql",
            name="test-source",
            created_at="2024-01-15T10:30:45Z",
            conn=conn
        )
        
        formatted = datasource.get_formatted_created_at()
        self.assertIn("2024-01-15", formatted)


class TestDataSourcesStore(unittest.TestCase):
    """Testes para a classe DataSourcesStore"""
    
    def setUp(self):
        """Configuração inicial para os testes"""
        # Criar uma nova instância para cada teste
        self.store = DataSourcesStore()
        # Limpar dados existentes
        self.store.datasources = []
    
    @patch('core.datasources_store.get_bridge_config_dir')
    def test_get_datasources_file_path(self, mock_get_config_dir):
        """Testa o caminho do arquivo de fontes de dados"""
        mock_get_config_dir.return_value = "C:\\test\\config"
        
        path = self.store.get_datasources_file_path()
        expected = "C:\\test\\config\\datasources.enc"
        self.assertEqual(path, expected)
    
    @patch('core.datasources_store.decrypt_data_from_file')
    @patch('os.path.exists')
    def test_load_existing_file(self, mock_exists, mock_decrypt):
        """Testa o carregamento de arquivo existente"""
        mock_exists.return_value = True
        mock_decrypt.return_value = {
            "version": 1,
            "sources": [
                {
                    "id": "test-id",
                    "type": "mysql",
                    "name": "test-source",
                    "created_at": "2024-01-01T00:00:00",
                    "conn": {
                        "host": "localhost",
                        "port": 3306,
                        "database": "test_db",
                        "user": "test_user",
                        "password": "test_pass",
                        "options": {}
                    },
                    "tables": {
                        "selected": [],
                        "last_discovery_at": None
                    }
                }
            ]
        }
        
        self.store.load()
        
        self.assertEqual(len(self.store.datasources), 1)
        self.assertEqual(self.store.datasources[0].name, "test-source")
    
    @patch('os.path.exists')
    def test_load_non_existing_file(self, mock_exists):
        """Testa o carregamento quando arquivo não existe"""
        mock_exists.return_value = False
        
        self.store.load()
        
        self.assertEqual(len(self.store.datasources), 0)
    
    @patch('core.datasources_store.encrypt_data_to_file')
    def test_save(self, mock_encrypt):
        """Testa o salvamento de dados"""
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            database="test_db",
            user="test_user",
            password="test_pass"
        )
        
        datasource = DataSource(
            id="test-id",
            type="mysql",
            name="test-source",
            created_at="2024-01-01T00:00:00",
            conn=conn
        )
        
        self.store.datasources = [datasource]
        self.store.save()
        
        mock_encrypt.assert_called_once()
    
    def test_add_datasource(self):
        """Testa a adição de uma fonte de dados"""
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            database="test_db",
            user="test_user",
            password="test_pass"
        )
        
        with patch.object(self.store, 'save'):
            datasource = self.store.add_datasource("test-source", "mysql", conn)
        
        self.assertEqual(len(self.store.datasources), 1)
        self.assertEqual(datasource.name, "test-source")
        self.assertEqual(datasource.type, "mysql")
        self.assertIsNotNone(datasource.id)
    
    def test_add_duplicate_datasource(self):
        """Testa a adição de fonte de dados duplicada"""
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            database="test_db",
            user="test_user",
            password="test_pass"
        )
        
        with patch.object(self.store, 'save'):
            self.store.add_datasource("test-source", "mysql", conn)
            
            with self.assertRaises(ValueError) as context:
                self.store.add_datasource("test-source", "postgresql", conn)
            
            self.assertIn("Já existe uma fonte de dados", str(context.exception))
    
    def test_delete_datasource(self):
        """Testa a remoção de uma fonte de dados"""
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            database="test_db",
            user="test_user",
            password="test_pass"
        )
        
        with patch.object(self.store, 'save'):
            self.store.add_datasource("test-source", "mysql", conn)
            
            result = self.store.delete_datasource("test-source")
            
            self.assertTrue(result)
            self.assertEqual(len(self.store.datasources), 0)
    
    def test_delete_non_existing_datasource(self):
        """Testa a remoção de fonte de dados inexistente"""
        result = self.store.delete_datasource("non-existing")
        
        self.assertFalse(result)
    
    def test_get_datasource_by_name(self):
        """Testa a busca por nome"""
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            database="test_db",
            user="test_user",
            password="test_pass"
        )
        
        with patch.object(self.store, 'save'):
            added_datasource = self.store.add_datasource("test-source", "mysql", conn)
            
            found_datasource = self.store.get_datasource_by_name("test-source")
            
            self.assertIsNotNone(found_datasource)
            self.assertEqual(found_datasource.name, "test-source")
            self.assertEqual(found_datasource.id, added_datasource.id)
    
    def test_get_non_existing_datasource_by_name(self):
        """Testa a busca por nome inexistente"""
        result = self.store.get_datasource_by_name("non-existing")
        
        self.assertIsNone(result)
    
    def test_list_datasources(self):
        """Testa a listagem de fontes de dados"""
        conn1 = DatabaseConnection(
            host="localhost",
            port=3306,
            database="test_db1",
            user="test_user",
            password="test_pass"
        )
        
        conn2 = DatabaseConnection(
            host="localhost",
            port=5432,
            database="test_db2",
            user="test_user",
            password="test_pass"
        )
        
        with patch.object(self.store, 'save'):
            self.store.add_datasource("source1", "mysql", conn1)
            self.store.add_datasource("source2", "postgresql", conn2)
            
            datasources = self.store.list_datasources()
            
            self.assertEqual(len(datasources), 2)
            names = [ds.name for ds in datasources]
            self.assertIn("source1", names)
            self.assertIn("source2", names)
    
    def test_save_selected_tables(self):
        """Testa o salvamento de tabelas selecionadas"""
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            database="test_db",
            user="test_user",
            password="test_pass"
        )
        
        with patch.object(self.store, 'save'):
            self.store.add_datasource("test-source", "mysql", conn)
            
            result = self.store.save_selected_tables("test-source", ["users", "orders"])
            
            self.assertTrue(result)
            
            datasource = self.store.get_datasource_by_name("test-source")
            self.assertEqual(datasource.tables.selected, ["users", "orders"])
            self.assertIsNotNone(datasource.tables.last_discovery_at)
    
    def test_get_datasources_count(self):
        """Testa a contagem de fontes de dados"""
        self.assertEqual(self.store.get_datasources_count(), 0)
        
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            database="test_db",
            user="test_user",
            password="test_pass"
        )
        
        with patch.object(self.store, 'save'):
            self.store.add_datasource("test-source", "mysql", conn)
            
            self.assertEqual(self.store.get_datasources_count(), 1)


if __name__ == '__main__':
    unittest.main()