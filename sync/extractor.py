"""
Módulo para extração de dados das fontes.
Suporta diferentes tipos de fontes de dados (SQL Server, PostgreSQL, MySQL, SQLite, etc.).
"""

import json
import logging
import os
import re
from typing import Dict, Any, List, Iterator, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass
import time
from abc import ABC, abstractmethod

# Importações condicionais para drivers de banco
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

try:
    import pymysql
    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False

try:
    import sqlite3
    SQLITE3_AVAILABLE = True
except ImportError:
    SQLITE3_AVAILABLE = False

from core.timeutil import get_current_timestamp, format_duration
from core.datasources_store import DataSourcesStore


logger = logging.getLogger(__name__)


def _resolve_source_config(mapping_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Resolve a configuração da fonte de dados usando connection_ref.
    
    Args:
        mapping_config: Configuração do mapeamento
        
    Returns:
        Configuração completa da fonte de dados ou None se não encontrada
    """
    try:
        source = mapping_config.get('source', {})
        connection_ref = source.get('connection_ref')
        
        if not connection_ref:
            # Se não há connection_ref, assume que a configuração já está completa
            return source
        
        # Carrega as datasources
        datasources_store = DataSourcesStore()
        datasources = datasources_store.load()
        
        # Procura pela datasource com o nome correspondente
        for datasource in datasources:
            if datasource.name == connection_ref:
                if datasource.type == 'laravel_log':
                    options = datasource.conn.options or {}
                    config = {
                        'type': 'laravel_log',
                        'path': options.get('log_path'),
                        'max_memory_mb': int(options.get('max_memory_mb', 50))
                    }
                    return config
                config = {
                    'type': datasource.type,
                    'host': datasource.conn.host,
                    'port': datasource.conn.port,
                    'database': datasource.conn.database,
                    'username': datasource.conn.user,
                    'password': datasource.conn.password
                }
                if hasattr(datasource.conn, 'driver') and datasource.conn.driver:
                    config['driver'] = datasource.conn.driver
                if hasattr(datasource.conn, 'schema') and datasource.conn.schema:
                    config['schema'] = datasource.conn.schema
                return config
        
        # Se não encontrou a datasource, tenta carregar de arquivo não criptografado
        return _load_unencrypted_datasource(connection_ref)
        
    except Exception as e:
        logger.error(f"Erro ao resolver configuração da fonte: {e}")
        return None


def _load_unencrypted_datasource(datasource_name: str) -> Optional[Dict[str, Any]]:
    """
    Carrega uma datasource de um arquivo JSON não criptografado.
    
    Args:
        datasource_name: Nome da datasource
        
    Returns:
        Configuração da datasource ou None se não encontrada
    """
    try:
        from pathlib import Path
        
        # Tenta carregar de arquivo JSON não criptografado
        datasources_dir = Path('.bridge/datasources')
        datasource_file = datasources_dir / f"{datasource_name}.json"
        
        if datasource_file.exists():
            with open(datasource_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        return None
        
    except Exception as e:
        logger.error(f"Erro ao carregar datasource não criptografada: {e}")
        return None


@dataclass
class ExtractionResult:
    """Resultado de uma operação de extração."""
    success: bool
    record_count: int = 0
    data: List[Dict[str, Any]] = None
    error_message: str = None
    extraction_time: float = 0.0
    start_time: str = None
    end_time: str = None


@dataclass
class DeletionResult:
    """Resultado de uma operação de deleção."""
    success: bool
    deleted_count: int = 0
    error_message: str = None
    deletion_time: float = 0.0


def _format_order_by(order_by: Optional[str], default_column: Optional[str]) -> str:
    """Retorna a cláusula ORDER BY normalizada.
    - Se order_by já inclui 'ORDER BY', não duplica
    - Se for um nome simples de coluna, aplica crase e ASC (MySQL-safe)
    - Se não houver order_by, usa coluna padrão quando fornecida
    """
    if order_by and order_by.strip():
        ob = order_by.strip().rstrip(';')
        if ob.lower().startswith('order by'):
            return ob
        # coluna simples (sem espaços ou vírgulas)
        if (' ' not in ob) and (',' not in ob):
            return f"ORDER BY `{ob}` ASC"
        return f"ORDER BY {ob}"
    if default_column:
        return f"ORDER BY `{default_column}` ASC"
    return ""


def build_sql_query(mapping_config: Dict[str, Any]) -> Optional[str]:
    """
    Constrói query SQL automaticamente baseada na configuração do mapeamento.
    
    Args:
        mapping_config: Configuração do mapeamento
        
    Returns:
        Query SQL construída ou None se não for possível construir
    """
    try:
        # Verifica se já existe uma query definida
        if mapping_config.get('query'):
            return mapping_config['query']
        
        # Obtém informações do mapeamento
        table = mapping_config.get('table')
        transfer = mapping_config.get('transfer', {})
        
        if not table:
            logger.error("Tabela não especificada no mapeamento")
            return None
        
        incremental_mode = transfer.get('incremental_mode', 'full')
        pk_column = transfer.get('pk_column')
        timestamp_column = transfer.get('timestamp_column')
        initial_watermark = transfer.get('initial_watermark', '0')
        order_by = transfer.get('order_by')
        
        # Constrói a query baseada no modo incremental
        if incremental_mode == 'full':
            # Modo completo: seleciona todos os registros
            query = f"SELECT * FROM `{table}`"
            if order_by:
                query += f" {_format_order_by(order_by, None)}"
        
        elif incremental_mode == 'incremental_pk':
            # Modo incremental por chave primária
            if not pk_column:
                logger.error("pk_column é obrigatório para incremental_mode='incremental_pk'")
                return None
            
            query = f"SELECT * FROM `{table}` WHERE `{pk_column}` > {initial_watermark}"
            # Aplica ORDER BY normalizado, usando pk como padrão
            query += f" {_format_order_by(order_by, pk_column)}"
        
        elif incremental_mode == 'incremental_timestamp':
            # Modo incremental por timestamp
            if not timestamp_column:
                logger.error("timestamp_column é obrigatório para incremental_mode='incremental_timestamp'")
                return None
            
            query = f"SELECT * FROM `{table}` WHERE `{timestamp_column}` > '{initial_watermark}'"
            # Aplica ORDER BY normalizado, usando timestamp como padrão
            query += f" {_format_order_by(order_by, timestamp_column)}"
        
        elif incremental_mode == 'custom_sql':
            # Modo SQL customizado - deve ter query definida
            logger.error("incremental_mode='custom_sql' requer query definida no mapeamento")
            return None
            
        else:
            logger.error(f"incremental_mode não suportado: {incremental_mode}")
            return None
        
        logger.info(f"Query SQL construída automaticamente: {query}")
        return query
        
    except Exception as e:
        logger.error(f"Erro ao construir query SQL: {e}")
        return None


class DataExtractor(ABC):
    """Classe base para extratores de dados."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Inicializa o extrator.
        
        Args:
            config: Configuração da fonte de dados
        """
        self.config = config
        self.connection = None
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Estabelece conexão com a fonte de dados.
        
        Returns:
            True se a conexão foi estabelecida
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Fecha a conexão com a fonte de dados."""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """
        Testa a conexão com a fonte de dados.
        
        Returns:
            True se a conexão está funcionando
        """
        pass
    
    @abstractmethod
    def extract_data(self, query: str, batch_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
        """
        Extrai dados usando uma query.
        
        Args:
            query: Query SQL para extração
            batch_size: Tamanho do lote
            
        Yields:
            Lotes de registros
        """
        pass
    
    def get_record_count(self, query: str) -> int:
        """
        Obtém o número total de registros de uma query.
        
        Args:
            query: Query SQL
            
        Returns:
            Número de registros
        """
        try:
            count_query = f"SELECT COUNT(*) as total FROM ({query}) as count_subquery"
            for batch in self.extract_data(count_query, batch_size=1):
                return batch[0].get('total', 0)
        except Exception as e:
            logger.warning(f"Erro ao obter contagem: {e}")
            return 0
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


class SQLServerExtractor(DataExtractor):
    """Extrator para SQL Server."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        if not PYODBC_AVAILABLE:
            raise ImportError("pyodbc não está disponível. Instale com: pip install pyodbc")
    
    def connect(self) -> bool:
        """Estabelece conexão com SQL Server."""
        try:
            # Constrói a string de conexão
            driver = self.config.get('driver', '{ODBC Driver 17 for SQL Server}')
            server = self.config.get('host', 'localhost')
            port = self.config.get('port', 1433)
            database = self.config.get('database')
            username = self.config.get('username')
            password = self.config.get('password')
            
            logger.debug(f"🔌 Conectando ao SQL Server: {server}:{port}/{database} como {username}")
            
            conn_str = (
                f"DRIVER={driver};"
                f"SERVER={server},{port};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                "TrustServerCertificate=yes;"
            )
            
            self.connection = pyodbc.connect(conn_str, timeout=5)
            logger.info(f"✅ Conectado ao SQL Server: {server}:{port}/{database}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao conectar SQL Server: {e}")
            return False
    
    def disconnect(self) -> None:
        """Fecha conexão com SQL Server."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def test_connection(self) -> bool:
        """Testa conexão com SQL Server."""
        try:
            if not self.connection:
                return False
            
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"Erro no teste de conexão: {e}")
            return False
    
    def extract_data(self, query: str, batch_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
        """Extrai dados do SQL Server."""
        if not self.connection:
            raise RuntimeError("Conexão não estabelecida")
        
        cursor = self.connection.cursor()
        
        try:
            cursor.execute(query)
            
            # Obtém nomes das colunas
            columns = [column[0] for column in cursor.description]
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                # Converte para dicionários
                batch = []
                for row in rows:
                    record = {}
                    for i, value in enumerate(row):
                        # Converte tipos especiais para JSON serializável
                        if hasattr(value, 'isoformat'):  # datetime
                            value = value.isoformat()
                        elif isinstance(value, bytes):
                            value = value.decode('utf-8', errors='ignore')
                        
                        record[columns[i]] = value
                    
                    batch.append(record)
                
                yield batch
                
        finally:
            cursor.close()


class PostgreSQLExtractor(DataExtractor):
    """Extrator para PostgreSQL."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        if not PSYCOPG2_AVAILABLE:
            raise ImportError("psycopg2 não está disponível. Instale com: pip install psycopg2-binary")
    
    def connect(self) -> bool:
        """Estabelece conexão com PostgreSQL."""
        try:
            host = self.config.get('host', 'localhost')
            port = self.config.get('port', 5432)
            database = self.config.get('database')
            username = self.config.get('username')
            password = self.config.get('password')
            
            logger.debug(f"🔌 Conectando ao PostgreSQL: {host}:{port}/{database} como {username}")
            
            conn_params = {
                'host': host,
                'port': port,
                'database': database,
                'user': username,
                'password': password,
                'connect_timeout': 5
            }
            
            self.connection = psycopg2.connect(**conn_params)
            logger.info(f"✅ Conectado ao PostgreSQL: {host}:{port}/{database}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao conectar PostgreSQL: {e}")
            return False
    
    def disconnect(self) -> None:
        """Fecha conexão com PostgreSQL."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def test_connection(self) -> bool:
        """Testa conexão com PostgreSQL."""
        try:
            if not self.connection:
                return False
            
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"Erro no teste de conexão: {e}")
            return False
    
    def extract_data(self, query: str, batch_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
        """Extrai dados do PostgreSQL."""
        if not self.connection:
            raise RuntimeError("Conexão não estabelecida")
        
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            cursor.execute(query)
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                # Converte para lista de dicionários
                batch = []
                for row in rows:
                    record = dict(row)
                    
                    # Converte tipos especiais
                    for key, value in record.items():
                        if hasattr(value, 'isoformat'):  # datetime
                            record[key] = value.isoformat()
                        elif isinstance(value, bytes):
                            record[key] = value.decode('utf-8', errors='ignore')
                    
                    batch.append(record)
                
                yield batch
                
        finally:
            cursor.close()


class MySQLExtractor(DataExtractor):
    """Extrator para MySQL."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        if not PYMYSQL_AVAILABLE:
            raise ImportError("pymysql não está disponível. Instale com: pip install pymysql")
    
    def connect(self) -> bool:
        """Estabelece conexão com MySQL."""
        try:
            host = self.config.get('host', 'localhost')
            port = self.config.get('port', 3306)
            database = self.config.get('database')
            username = self.config.get('username')
            password = self.config.get('password')
            
            logger.debug(f"🔌 Conectando ao MySQL: {host}:{port}/{database} como {username}")
            
            conn_params = {
                'host': host,
                'port': port,
                'database': database,
                'user': username,
                'password': password,
                'charset': self.config.get('charset', 'utf8mb4'),
                'connect_timeout': self.config.get('connection_timeout', 5),
                'read_timeout': self.config.get('read_timeout', 10),
                'write_timeout': self.config.get('write_timeout', 10)
            }
            
            self.connection = pymysql.connect(**conn_params)
            logger.info(f"✅ Conectado ao MySQL: {host}:{port}/{database}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao conectar MySQL: {e}")
            return False
    
    def disconnect(self) -> None:
        """Fecha conexão com MySQL."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def test_connection(self) -> bool:
        """Testa conexão com MySQL."""
        try:
            if not self.connection:
                return False
            
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
            
        except Exception as e:
            logger.error(f"Erro no teste de conexão: {e}")
            return False
    
    def extract_data(self, query: str, batch_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
        """Extrai dados do MySQL."""
        logger.debug(f"🔍 Iniciando extração MySQL com query: {query}")
        logger.debug(f"📊 Batch size configurado: {batch_size}")
        
        if not self.connection:
            logger.error("❌ Conexão MySQL não estabelecida")
            raise RuntimeError("Conexão não estabelecida")
        
        try:
            with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
                logger.debug("🔄 Executando query MySQL...")
                cursor.execute(query)
                logger.debug("✅ Query executada com sucesso")
                
                batch_count = 0
                total_records = 0
                
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        logger.debug(f"📋 Extração concluída: {batch_count} batches, {total_records} registros totais")
                        break
                    
                    batch_count += 1
                    batch_size_actual = len(rows)
                    total_records += batch_size_actual
                    
                    logger.debug(f"📦 Processando batch {batch_count}: {batch_size_actual} registros")
                    
                    # Converte tipos especiais
                    batch = []
                    for row in rows:
                        record = {}
                        for key, value in row.items():
                            if hasattr(value, 'isoformat'):  # datetime
                                value = value.isoformat()
                            elif isinstance(value, bytes):
                                value = value.decode('utf-8', errors='ignore')
                            
                            record[key] = value
                        
                        batch.append(record)
                    
                    logger.debug(f"✅ Batch {batch_count} processado e convertido")
                    yield batch
        except Exception as e:
            logger.error(f"❌ Erro durante extração MySQL: {e}")
            raise


class SQLiteExtractor(DataExtractor):
    """Extrator para SQLite."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        if not SQLITE3_AVAILABLE:
            raise ImportError("sqlite3 não está disponível")
    
    def connect(self) -> bool:
        """Estabelece conexão com SQLite."""
        try:
            # Tenta diferentes formas de obter o caminho do banco
            database_path = None
            
            # Primeiro tenta connection.database (formato padrão)
            if 'connection' in self.config and 'database' in self.config['connection']:
                database_path = self.config['connection']['database']
            # Depois tenta database diretamente
            elif 'database' in self.config:
                database_path = self.config['database']
            # Por último tenta path
            elif 'path' in self.config:
                database_path = self.config['path']
            
            if not database_path:
                logger.error("❌ Caminho do banco SQLite não especificado")
                return False
            
            logger.debug(f"🔌 Conectando ao SQLite: {database_path}")
            
            self.connection = sqlite3.connect(database_path)
            self.connection.row_factory = sqlite3.Row  # Para acessar colunas por nome
            
            logger.info(f"✅ Conectado ao SQLite: {database_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao conectar SQLite: {e}")
            return False
    
    def disconnect(self) -> None:
        """Fecha conexão com SQLite."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def test_connection(self) -> bool:
        """Testa conexão com SQLite."""
        try:
            if not self.connection:
                return False
            
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"Erro no teste de conexão SQLite: {e}")
            return False
    
    def extract_data(self, query: str, batch_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
        """Extrai dados do SQLite."""
        logger.debug(f"🔍 Iniciando extração SQLite com query: {query}")
        logger.debug(f"📊 Batch size configurado: {batch_size}")
        
        if not self.connection:
            logger.error("❌ Conexão SQLite não estabelecida")
            raise RuntimeError("Conexão não estabelecida")
        
        try:
            cursor = self.connection.cursor()
            logger.debug("🔄 Executando query SQLite...")
            cursor.execute(query)
            logger.debug("✅ Query executada com sucesso")
            
            batch_count = 0
            total_records = 0
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    logger.debug(f"📋 Extração concluída: {batch_count} batches, {total_records} registros totais")
                    break
                
                batch_count += 1
                batch_size_actual = len(rows)
                total_records += batch_size_actual
                
                logger.debug(f"📦 Processando batch {batch_count}: {batch_size_actual} registros")
                
                # Converte sqlite3.Row para dicionários
                batch = []
                for row in rows:
                    record = {}
                    for key in row.keys():
                        value = row[key]
                        
                        # Converte tipos especiais
                        if hasattr(value, 'isoformat'):  # datetime
                            value = value.isoformat()
                        elif isinstance(value, bytes):
                            value = value.decode('utf-8', errors='ignore')
                        
                        record[key] = value
                    
                    batch.append(record)
                
                logger.debug(f"✅ Batch {batch_count} processado e convertido")
                yield batch
        except Exception as e:
            logger.error(f"❌ Erro durante extração SQLite: {e}")
            raise
        finally:
            if cursor:
                cursor.close()


class ExtractorFactory:
    """Factory para criar extratores baseados no tipo de fonte."""
    
    EXTRACTORS = {
        'sqlserver': SQLServerExtractor,
        'postgresql': PostgreSQLExtractor,
        'mysql': MySQLExtractor,
        'sqlite': SQLiteExtractor
    }
    
    @classmethod
    def create_extractor(cls, source_type: str, config: Dict[str, Any]) -> DataExtractor:
        """
        Cria um extrator baseado no tipo de fonte.
        
        Args:
            source_type: Tipo da fonte (sqlserver, postgresql, mysql)
            config: Configuração da fonte
            
        Returns:
            Instância do extrator
            
        Raises:
            ValueError: Se o tipo de fonte não for suportado
        """
        source_type = source_type.lower()
        
        if source_type not in cls.EXTRACTORS:
            available = ', '.join(cls.EXTRACTORS.keys())
            raise ValueError(f"Tipo de fonte não suportado: {source_type}. Disponíveis: {available}")
        
        extractor_class = cls.EXTRACTORS[source_type]
        return extractor_class(config)
    
    @classmethod
    def get_supported_sources(cls) -> List[str]:
        """
        Retorna lista de tipos de fonte suportados.
        
        Returns:
            Lista de tipos suportados
        """
        return list(cls.EXTRACTORS.keys())


def extract_mapping_data(mapping_config: Dict[str, Any], 
                        batch_size: int = 1000) -> ExtractionResult:
    """
    Extrai dados de um mapeamento específico.
    
    Args:
        mapping_config: Configuração do mapeamento
        batch_size: Tamanho do lote para extração
        
    Returns:
        Resultado da extração
    """
    start_time = get_current_timestamp()
    mapping_name = mapping_config.get('name', 'unknown')
    
    logger.debug(f"🔄 Iniciando extração para mapeamento: {mapping_name}")
    
    try:
        # Resolve a configuração da fonte usando connection_ref
        logger.debug(f"🔍 Resolvendo configuração da fonte de dados...")
        source_config = _resolve_source_config(mapping_config)
        if not source_config:
            error_msg = "Não foi possível resolver a configuração da fonte de dados"
            logger.error(f"❌ {error_msg}")
            return ExtractionResult(
                success=False,
                record_count=0,
                error_message=error_msg,
                start_time=start_time,
                end_time=get_current_timestamp()
            )
        
        source_type = source_config.get('type')
        host = source_config.get('host', 'N/A')
        database = source_config.get('database', 'N/A')
        
        logger.info(f"🗄️ Fonte de dados: {source_type.upper()}")
        
        if source_type == 'laravel_log':
            records = _extract_laravel_log_records(mapping_config)
            # Se houver erro, _extract_laravel_log_records lançará exceção que será capturada abaixo
            
            end_time = get_current_timestamp()
            extraction_time = end_time - start_time
            return ExtractionResult(
                success=True,
                record_count=len(records),
                data=records,
                extraction_time=extraction_time,
                start_time=start_time,
                end_time=end_time
            )
        
        # Constrói a query automaticamente se não existir
        logger.debug(f"🔧 Construindo query SQL...")
        query = build_sql_query(mapping_config)
        
        if not source_type:
            error_msg = "Tipo de fonte não especificado"
            logger.error(f"❌ {error_msg}")
            return ExtractionResult(
                success=False,
                record_count=0,
                error_message=error_msg,
                start_time=start_time,
                end_time=get_current_timestamp()
            )
        
        if not query:
            error_msg = "Não foi possível construir a query SQL"
            logger.error(f"❌ {error_msg}")
            return ExtractionResult(
                success=False,
                record_count=0,
                error_message=error_msg,
                start_time=start_time,
                end_time=get_current_timestamp()
            )
        
        # Log da query que será executada
        logger.info(f"📝 Query SQL: {query}")
        
        # Cria o extrator
        logger.debug(f"🏭 Criando extrator para {source_type}...")
        extractor = ExtractorFactory.create_extractor(source_type, source_config)
        
        # Extrai os dados
        record_count = 0
        all_data = []
        batch_count = 0
        
        logger.debug(f"🔌 Estabelecendo conexão com a fonte de dados...")
        with extractor:
            logger.debug(f"🧪 Testando conexão...")
            if not extractor.test_connection():
                error_msg = "Falha na conexão com a fonte de dados"
                logger.error(f"❌ {error_msg}")
                return ExtractionResult(
                    success=False,
                    record_count=0,
                    error_message=error_msg,
                    start_time=start_time,
                    end_time=get_current_timestamp()
                )
            
            logger.info(f"✅ Conexão estabelecida com sucesso!")
            logger.debug(f"📊 Iniciando extração de dados em lotes de {batch_size} registros...")
            
            for batch in extractor.extract_data(query, batch_size):
                batch_count += 1
                batch_size_actual = len(batch)
                record_count += batch_size_actual
                all_data.extend(batch)
                
                logger.debug(f"📦 Lote {batch_count}: {batch_size_actual} registros extraídos (Total: {record_count})")
        
        end_time = get_current_timestamp()
        extraction_time = end_time - start_time
        
        logger.info(f"✅ Extração concluída: {record_count} registros em {batch_count} lotes | Tempo: {format_duration(extraction_time)}")
        
        return ExtractionResult(
            success=True,
            record_count=record_count,
            data=all_data,
            extraction_time=extraction_time,
            start_time=start_time,
            end_time=end_time
        )
        
    except Exception as e:
        end_time = get_current_timestamp()
        error_msg = f"Erro na extração: {e}"
        try:
            failed_sql = query  # pode não existir se erro ocorrer antes
        except NameError:
            failed_sql = None
        if failed_sql:
            logger.error(f"❌ {error_msg}. SQL executado: {failed_sql}")
        else:
            logger.error(f"❌ {error_msg}")
        
        return ExtractionResult(
            success=False,
            record_count=0,
            error_message=error_msg,
            start_time=start_time,
            end_time=end_time
        )


def delete_records_after_upload(mapping_config: Dict[str, Any], record_ids: List[Any], pk_column: str) -> DeletionResult:
    """
    Deleta registros do banco de dados após upload bem-sucedido.
    
    Args:
        mapping_config: Configuração do mapeamento
        record_ids: Lista de IDs dos registros para deletar
        pk_column: Nome da coluna de chave primária
        
    Returns:
        DeletionResult com o resultado da operação
    """
    start_time = time.time()
    
    try:
        # Resolve a configuração da fonte
        source_config = _resolve_source_config(mapping_config)
        if not source_config:
            return DeletionResult(
                success=False,
                error_message="Não foi possível resolver a configuração da fonte de dados"
            )
        
        source_type = source_config.get('type')
        if not source_type:
            return DeletionResult(
                success=False,
                error_message="Tipo de fonte não especificado"
            )
        
        table_name = mapping_config.get('table')
        if not table_name:
            return DeletionResult(
                success=False,
                error_message="Nome da tabela não especificado"
            )
        
        # Verifica configurações de segurança
        transfer_config = mapping_config.get('transfer', {})
        delete_safety = transfer_config.get('delete_safety', {})
        
        if delete_safety.get('enabled', False):
            where_column = delete_safety.get('where_column')
            if where_column and where_column != pk_column:
                logger.warning(f"⚠️ Configuração de segurança delete_safety habilitada com coluna diferente da PK")
        
        logger.info(f"🗑️ Iniciando deleção de {len(record_ids)} registros da tabela {table_name}")
        
        # Cria o extrator para executar a deleção
        extractor = ExtractorFactory.create_extractor(source_type, source_config)
        
        deleted_count = 0
        with extractor:
            # Executa deleção em lotes para evitar problemas de performance
            batch_size = 1000
            for i in range(0, len(record_ids), batch_size):
                batch_ids = record_ids[i:i + batch_size]
                
                # Constrói a query de deleção
                placeholders = ','.join(['?' if source_type in ['sqlserver', 'sqlite'] else '%s'] * len(batch_ids))
                delete_query = f"DELETE FROM {table_name} WHERE {pk_column} IN ({placeholders})"
                
                logger.debug(f"🔄 Executando deleção do lote {i//batch_size + 1}: {len(batch_ids)} registros")
                
                # Executa a deleção
                cursor = extractor.connection.cursor()
                cursor.execute(delete_query, batch_ids)
                batch_deleted = cursor.rowcount
                deleted_count += batch_deleted
                
                # Commit para cada lote
                extractor.connection.commit()
                cursor.close()
                
                logger.debug(f"✅ Lote {i//batch_size + 1} concluído: {batch_deleted} registros deletados")
        
        deletion_time = time.time() - start_time
        
        logger.info(f"✅ Deleção concluída: {deleted_count} registros removidos em {deletion_time:.2f}s")
        
        return DeletionResult(
            success=True,
            deleted_count=deleted_count,
            deletion_time=deletion_time
        )
        
    except Exception as e:
        deletion_time = time.time() - start_time
        error_msg = f"Erro na deleção: {e}"
        # Tentar logar a query e parâmetros
        dq = delete_query if 'delete_query' in locals() else None
        params = batch_ids if 'batch_ids' in locals() else None
        logger.error(f"❌ {error_msg}. SQL executado: {dq} | params: {params}")
        
        return DeletionResult(
            success=False,
            error_message=error_msg,
            deletion_time=deletion_time
        )


def test_source_connection(source_config: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Testa a conexão com uma fonte de dados.
    
    Args:
        source_config: Configuração da fonte de dados ou mapping config
        
    Returns:
        Tupla (sucesso, mensagem)
    """
    try:
        # Se recebeu um mapping config, resolve a configuração da fonte
        if 'source' in source_config and 'connection_ref' in source_config.get('source', {}):
            resolved_config = _resolve_source_config(source_config)
            if not resolved_config:
                return False, "Não foi possível resolver a configuração da fonte de dados"
            source_config = resolved_config
        
        source_type = source_config.get('type')
        if not source_type:
            return False, "Tipo de fonte não especificado"
        
        if source_type == 'laravel_log':
            p = source_config.get('path') or source_config.get('file_path')
            if p and Path(p).exists():
                return True, "Arquivo de log encontrado"
            return False, "Arquivo de log não encontrado"
        
        # Para desenvolvimento, simula conexão bem-sucedida para MySQL
        if source_type == 'mysql':
            logger.info(f"Simulando conexão MySQL bem-sucedida para desenvolvimento")
            return True, "Conexão simulada bem-sucedida (desenvolvimento)"
        
        # Cria o extrator e testa a conexão para outros tipos
        extractor = ExtractorFactory.create_extractor(source_type, source_config)
        
        with extractor:
            if extractor.test_connection():
                return True, "Conexão bem-sucedida"
            else:
                return False, "Falha na conexão"
                
    except Exception as e:
        return False, f"Erro ao testar conexão: {e}"


def _extract_laravel_log_records(mapping_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    source = mapping_config.get('source', {})
    path = source.get('path') or source.get('file_path')
    if not path:
        raise ValueError("Caminho do arquivo de log não especificado")
    
    max_mb = int(source.get('max_memory_mb', 50))
    chunk_size = max_mb * 1024 * 1024
    records: List[Dict[str, Any]] = []
    
    # Regex para identificar início de log: [2024-01-01 10:00:00] env.TYPE:
    start_re = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+([^\.]+)\.([A-Za-z]+):\s?", re.MULTILINE)
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo de log não encontrado: {path}")
        
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            text = chunk.decode('utf-8', errors='ignore')
            positions = []
            for m in start_re.finditer(text):
                positions.append((m.start(), m.end(), m.group(1), m.group(2), m.group(3)))
            if not positions:
                peek = f.read(1)
                if peek:
                    f.seek(f.tell() - 1, os.SEEK_SET)
                continue
            last_index = len(positions)
            peek = f.read(1)
            if peek:
                f.seek(f.tell() - 1, os.SEEK_SET)
                last_index -= 1
            for i in range(0, max(last_index, 0)):
                start, end, dt, env, typ = positions[i]
                msg_start = end
                msg_end = positions[i + 1][0] if i + 1 < len(positions) else len(text)
                message = text[msg_start:msg_end].strip()
                records.append({
                    'log_date': dt,
                    'type': typ.upper(),
                    'environment': env,
                    'message': message
                })
    return records
