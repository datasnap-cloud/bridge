"""
Módulo para conectar e consultar bancos de dados
"""

import pymysql
from typing import Dict, List, Any, Optional, Tuple

from core.logger import logger


class DatabaseConnector:
    """Classe para conectar e consultar bancos de dados"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        """
        Inicializa o conector com configurações de conexão
        
        Args:
            connection_config: Dicionário com configurações de conexão
        """
        self.config = connection_config
        self.connection = None
    
    def connect(self) -> bool:
        """
        Estabelece conexão com o banco de dados
        
        Returns:
            bool: True se conectou com sucesso, False caso contrário
        """
        try:
            if self.config['type'] == 'mysql':
                self.connection = pymysql.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    user=self.config['username'],
                    password=self.config['password'],
                    database=self.config['database'],
                    charset='utf8mb4',
                    autocommit=True
                )
                logger.info(f"Conectado ao MySQL: {self.config['host']}:{self.config['port']}/{self.config['database']}")
                return True
            else:
                logger.error(f"Tipo de banco não suportado: {self.config['type']}")
                return False
                
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco: {e}")
            return False
    
    def disconnect(self) -> None:
        """Fecha a conexão com o banco de dados"""
        if self.connection and self.connection.open:
            self.connection.close()
            logger.info("Conexão com banco de dados fechada")
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Obtém informações das colunas de uma tabela
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            List[Dict]: Lista com informações das colunas
        """
        try:
            if not self.connection or not self.connection.open:
                if not self.connect():
                    return []
            
            cursor = self.connection.cursor(pymysql.cursors.DictCursor)
            
            # Query para obter informações das colunas
            query = """
                SELECT 
                    COLUMN_NAME as name,
                    DATA_TYPE as type,
                    IS_NULLABLE as nullable,
                    COLUMN_DEFAULT as default_value,
                    COLUMN_KEY as key_type,
                    EXTRA as extra
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """
            
            cursor.execute(query, (self.config['database'], table_name))
            columns = cursor.fetchall()
            cursor.close()
            
            logger.info(f"Obtidas {len(columns)} colunas da tabela {table_name}")
            return columns
            
        except Exception as e:
            logger.error(f"Erro ao obter colunas da tabela {table_name}: {e}")
            return []
    
    def get_primary_key_column(self, table_name: str) -> Optional[str]:
        """
        Obtém o nome da coluna de chave primária
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Optional[str]: Nome da coluna PK ou None se não encontrada
        """
        try:
            columns = self.get_table_columns(table_name)
            for column in columns:
                if column.get('key_type') == 'PRI':
                    return column['name']
            return None
            
        except Exception as e:
            logger.error(f"Erro ao obter chave primária da tabela {table_name}: {e}")
            return None
    
    def sample_table_data(self, table_name: str, limit: int = 100, order_by: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Obtém uma amostra de dados da tabela
        
        Args:
            table_name: Nome da tabela
            limit: Número máximo de registros
            order_by: Coluna para ordenação (opcional)
            
        Returns:
            List[Dict]: Lista com os dados amostrados
        """
        try:
            if not self.connection or not self.connection.open:
                if not self.connect():
                    return []
            
            cursor = self.connection.cursor(pymysql.cursors.DictCursor)
            
            # Construir query de amostragem
            query = f"SELECT * FROM `{table_name}`"
            
            if order_by:
                query += f" ORDER BY `{order_by}`"
            
            query += f" LIMIT {limit}"
            
            cursor.execute(query)
            data = cursor.fetchall()
            cursor.close()
            
            logger.info(f"Obtidos {len(data)} registros de amostra da tabela {table_name}")
            return data
            
        except Exception as e:
            logger.error(f"Erro ao amostrar dados da tabela {table_name}: {e}")
            return []
    
    def get_table_count(self, table_name: str) -> int:
        """
        Obtém o número total de registros na tabela
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            int: Número de registros
        """
        try:
            if not self.connection or not self.connection.open:
                if not self.connect():
                    return 0
            
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            count = cursor.fetchone()[0]
            cursor.close()
            
            return count
            
        except Exception as e:
            logger.error(f"Erro ao contar registros da tabela {table_name}: {e}")
            return 0
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Testa a conexão com o banco de dados
        
        Returns:
            Tuple[bool, str]: (sucesso, mensagem)
        """
        try:
            if self.connect():
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                self.disconnect()
                return True, "Conexão testada com sucesso"
            else:
                return False, "Falha ao conectar"
                
        except Exception as e:
            return False, f"Erro no teste de conexão: {e}"
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


def create_database_connector(datasource: Dict[str, Any]) -> DatabaseConnector:
    """
    Cria um conector de banco de dados a partir de uma fonte de dados
    
    Args:
        datasource: Dicionário com informações da fonte de dados
        
    Returns:
        DatabaseConnector: Instância do conector
    """
    connection_config = {
        'type': datasource['connection']['type'],
        'host': datasource['connection']['host'],
        'port': datasource['connection']['port'],
        'username': datasource['connection']['username'],
        'password': datasource['connection']['password'],
        'database': datasource['connection']['database']
    }
    
    return DatabaseConnector(connection_config)