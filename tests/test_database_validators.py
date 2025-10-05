"""
Testes unitários para o módulo core.database_validators
"""

import pytest
from unittest.mock import patch, MagicMock

from core.database_validators import (
    ValidationResult, TableInfo, MySQLValidator, PostgreSQLValidator, 
    DatabaseValidatorFactory
)


class TestValidationResult:
    """Testes para a classe ValidationResult"""
    
    def test_validation_result_success(self):
        """Testa criação de ValidationResult de sucesso"""
        result = ValidationResult(
            success=True,
            message="Conexão válida",
            connection_time_ms=150
        )
        
        assert result.success is True
        assert result.message == "Conexão válida"
        assert result.connection_time_ms == 150
        assert result.error_details is None
    
    def test_validation_result_failure(self):
        """Testa criação de ValidationResult de falha"""
        result = ValidationResult(
            success=False,
            message="Falha na conexão",
            error_details="Connection timeout"
        )
        
        assert result.success is False
        assert result.message == "Falha na conexão"
        assert result.connection_time_ms is None
        assert result.error_details == "Connection timeout"


class TestTableInfo:
    """Testes para a classe TableInfo"""
    
    def test_table_info_creation(self):
        """Testa criação de TableInfo"""
        table = TableInfo(
            name="users",
            type="BASE TABLE",
            comment="User accounts table"
        )
        
        assert table.name == "users"
        assert table.type == "BASE TABLE"
        assert table.comment == "User accounts table"


class TestMySQLValidator:
    """Testes para a classe MySQLValidator"""
    
    def setup_method(self):
        """Setup para cada teste"""
        self.validator = MySQLValidator()
    
    @patch('pymysql.connect')
    def test_validate_connection_success(self, mock_connect):
        """Testa validação de conexão MySQL bem-sucedida"""
        # Mock da conexão
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        result = self.validator.validate_connection(
            host="localhost",
            port=3306,
            database="testdb",
            user="testuser",
            password="testpass"
        )
        
        assert result.success is True
        assert "Conexão MySQL válida" in result.message
        assert result.connection_time_ms is not None
        
        # Verificar se a conexão foi chamada corretamente
        mock_connect.assert_called_once_with(
            host="localhost",
            port=3306,
            user="testuser",
            password="testpass",
            database="testdb",
            charset="utf8mb4",
            connect_timeout=8
        )
        
        # Verificar se SELECT 1 foi executado
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_conn.close.assert_called_once()
    
    @patch('pymysql.connect')
    def test_validate_connection_failure(self, mock_connect):
        """Testa validação de conexão MySQL com falha"""
        mock_connect.side_effect = Exception("Connection refused")
        
        result = self.validator.validate_connection(
            host="invalid-host",
            port=3306,
            database="testdb",
            user="testuser",
            password="testpass"
        )
        
        assert result.success is False
        assert "Não foi possível validar a conexão" in result.message
        assert "Connection refused" in result.error_details
    
    @patch('pymysql.connect')
    def test_discover_tables_success(self, mock_connect):
        """Testa descoberta de tabelas MySQL bem-sucedida"""
        # Mock da conexão
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock dos resultados
        mock_cursor.fetchall.return_value = [
            ("users", "BASE TABLE"),
            ("orders", "BASE TABLE"),
            ("temp_view", "VIEW")
        ]
        
        success, tables, error = self.validator.discover_tables(
            host="localhost",
            port=3306,
            database="testdb",
            user="testuser",
            password="testpass"
        )
        
        assert success is True
        assert len(tables) == 2  # Apenas BASE TABLE
        assert tables[0].name == "users"
        assert tables[1].name == "orders"
        assert error is None
        
        # Verificar se a query correta foi executada
        expected_query = "SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'"
        mock_cursor.execute.assert_called_with(expected_query)
    
    @patch('pymysql.connect')
    def test_discover_tables_failure(self, mock_connect):
        """Testa descoberta de tabelas MySQL com falha"""
        mock_connect.side_effect = Exception("Access denied")
        
        success, tables, error = self.validator.discover_tables(
            host="localhost",
            port=3306,
            database="testdb",
            user="testuser",
            password="testpass"
        )
        
        assert success is False
        assert tables == []
        assert "Access denied" in error


class TestPostgreSQLValidator:
    """Testes para a classe PostgreSQLValidator"""
    
    def setup_method(self):
        """Setup para cada teste"""
        self.validator = PostgreSQLValidator()
    
    @patch('psycopg2.connect')
    def test_validate_connection_success(self, mock_connect):
        """Testa validação de conexão PostgreSQL bem-sucedida"""
        # Mock da conexão
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        result = self.validator.validate_connection(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass"
        )
        
        assert result.success is True
        assert "Conexão PostgreSQL válida" in result.message
        assert result.connection_time_ms is not None
        
        # Verificar se a conexão foi chamada corretamente
        mock_connect.assert_called_once_with(
            host="localhost",
            port=5432,
            user="testuser",
            password="testpass",
            database="testdb",
            connect_timeout=8
        )
        
        # Verificar se SELECT 1 foi executado
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_conn.close.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_validate_connection_failure(self, mock_connect):
        """Testa validação de conexão PostgreSQL com falha"""
        mock_connect.side_effect = Exception("Connection refused")
        
        result = self.validator.validate_connection(
            host="invalid-host",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass"
        )
        
        assert result.success is False
        assert "Não foi possível validar a conexão" in result.message
        assert "Connection refused" in result.error_details
    
    @patch('psycopg2.connect')
    def test_discover_tables_success(self, mock_connect):
        """Testa descoberta de tabelas PostgreSQL bem-sucedida"""
        # Mock da conexão
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock dos resultados
        mock_cursor.fetchall.return_value = [
            ("users", "BASE TABLE", "User accounts"),
            ("orders", "BASE TABLE", "Customer orders"),
        ]
        
        success, tables, error = self.validator.discover_tables(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass"
        )
        
        assert success is True
        assert len(tables) == 2
        assert tables[0].name == "users"
        assert tables[0].comment == "User accounts"
        assert tables[1].name == "orders"
        assert error is None
        
        # Verificar se a query correta foi executada
        expected_query = """
        SELECT table_name, table_type, 
               COALESCE(obj_description(c.oid), '') as comment
        FROM information_schema.tables t
        LEFT JOIN pg_class c ON c.relname = t.table_name
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        mock_cursor.execute.assert_called_once()
        # Verificar se a query contém os elementos principais
        actual_query = mock_cursor.execute.call_args[0][0]
        assert "information_schema.tables" in actual_query
        assert "table_type = 'BASE TABLE'" in actual_query
    
    @patch('psycopg2.connect')
    def test_discover_tables_failure(self, mock_connect):
        """Testa descoberta de tabelas PostgreSQL com falha"""
        mock_connect.side_effect = Exception("Access denied")
        
        success, tables, error = self.validator.discover_tables(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass"
        )
        
        assert success is False
        assert tables == []
        assert "Access denied" in error


class TestDatabaseValidatorFactory:
    """Testes para a classe DatabaseValidatorFactory"""
    
    def test_get_validator_mysql(self):
        """Testa obtenção de validador MySQL"""
        validator = DatabaseValidatorFactory.get_validator("mysql")
        assert isinstance(validator, MySQLValidator)
    
    def test_get_validator_postgresql(self):
        """Testa obtenção de validador PostgreSQL"""
        validator = DatabaseValidatorFactory.get_validator("postgresql")
        assert isinstance(validator, PostgreSQLValidator)
    
    def test_get_validator_invalid(self):
        """Testa obtenção de validador inválido"""
        with pytest.raises(ValueError, match="Tipo de banco de dados não suportado"):
            DatabaseValidatorFactory.get_validator("oracle")
    
    @patch.object(MySQLValidator, 'validate_connection')
    def test_validate_connection_mysql(self, mock_validate):
        """Testa validação de conexão via factory para MySQL"""
        mock_result = ValidationResult(True, "Success", 100)
        mock_validate.return_value = mock_result
        
        result = DatabaseValidatorFactory.validate_connection(
            "mysql", "localhost", 3306, "db", "user", "pass"
        )
        
        assert result == mock_result
        mock_validate.assert_called_once_with(
            "localhost", 3306, "db", "user", "pass"
        )
    
    @patch.object(PostgreSQLValidator, 'discover_tables')
    def test_discover_tables_postgresql(self, mock_discover):
        """Testa descoberta de tabelas via factory para PostgreSQL"""
        mock_tables = [TableInfo("users", "BASE TABLE")]
        mock_discover.return_value = (True, mock_tables, None)
        
        success, tables, error = DatabaseValidatorFactory.discover_tables(
            "postgresql", "localhost", 5432, "db", "user", "pass"
        )
        
        assert success is True
        assert tables == mock_tables
        assert error is None
        mock_discover.assert_called_once_with(
            "localhost", 5432, "db", "user", "pass"
        )