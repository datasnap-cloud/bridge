"""
Validadores de conexão de banco de dados
Suporta MySQL e PostgreSQL com validação robusta
"""

import time
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass

from core.logger import logger


@dataclass
class ValidationResult:
    """Resultado da validação de conexão"""
    success: bool
    message: str
    error_details: Optional[str] = None
    connection_time_ms: Optional[int] = None


@dataclass
class TableInfo:
    """Informações sobre uma tabela"""
    name: str
    type: Optional[str] = None  # 'BASE TABLE', 'VIEW', etc.
    comment: Optional[str] = None


class MySQLValidator:
    """Validador de conexões MySQL"""
    
    @staticmethod
    def validate_connection(host: str, port: int, database: str, user: str, password: str) -> ValidationResult:
        """
        Valida uma conexão MySQL
        
        Args:
            host: Endereço do servidor
            port: Porta do servidor
            database: Nome do banco de dados
            user: Usuário
            password: Senha
            
        Returns:
            ValidationResult: Resultado da validação
        """
        logger.debug(f"🔍 Validando conexão MySQL: {user}@{host}:{port}/{database}")
        
        try:
            import pymysql
        except ImportError:
            logger.error("❌ pymysql não está instalado")
            return ValidationResult(
                success=False,
                message="Dependência pymysql não encontrada. Execute: pip install pymysql",
                error_details="ImportError: pymysql"
            )
        
        start_time = time.time()
        
        try:
            # Configurar conexão
            connection_config = {
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'database': database,
                'charset': 'utf8mb4',
                'connect_timeout': 8,
                'read_timeout': 10,
                'write_timeout': 10,
                'autocommit': True
            }
            
            # Tentar conectar
            connection = pymysql.connect(**connection_config)
            
            try:
                # Executar teste básico
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    
                    if result and result[0] == 1:
                        connection_time = int((time.time() - start_time) * 1000)
                        logger.debug(f"✅ Conexão MySQL validada em {connection_time}ms")
                        
                        return ValidationResult(
                            success=True,
                            message="Conexão validada com sucesso",
                            connection_time_ms=connection_time
                        )
                    else:
                        return ValidationResult(
                            success=False,
                            message="Falha no teste de conectividade",
                            error_details="SELECT 1 retornou resultado inesperado"
                        )
                        
            finally:
                connection.close()
                
        except pymysql.Error as e:
            error_code = getattr(e, 'args', [None])[0] if hasattr(e, 'args') and e.args else None
            error_msg = str(e)
            
            logger.warning(f"⚠️ Erro de conexão MySQL ({error_code}): {error_msg}")
            
            # Mapear erros comuns para mensagens amigáveis
            if error_code == 1045:  # Access denied
                message = "Acesso negado. Verifique usuário e senha."
            elif error_code == 1049:  # Unknown database
                message = f"Banco de dados '{database}' não encontrado."
            elif error_code == 2003:  # Can't connect to server
                message = f"Não foi possível conectar ao servidor {host}:{port}."
            elif error_code == 1044:  # Access denied for user to database
                message = f"Usuário '{user}' não tem acesso ao banco '{database}'."
            else:
                message = "Não foi possível validar a conexão. Verifique os dados e tente novamente."
            
            return ValidationResult(
                success=False,
                message=message,
                error_details=f"MySQL Error {error_code}: {error_msg}" if error_code else str(e)
            )
            
        except Exception as e:
            logger.exception(f"❌ Erro inesperado na validação MySQL: {e}")
            return ValidationResult(
                success=False,
                message="Erro inesperado na validação. Verifique os dados e tente novamente.",
                error_details=str(e)
            )
    
    @staticmethod
    def discover_tables(host: str, port: int, database: str, user: str, password: str) -> Tuple[bool, List[TableInfo], str]:
        """
        Descobre tabelas disponíveis no banco MySQL
        
        Args:
            host: Endereço do servidor
            port: Porta do servidor
            database: Nome do banco de dados
            user: Usuário
            password: Senha
            
        Returns:
            Tuple[bool, List[TableInfo], str]: (sucesso, lista de tabelas, mensagem de erro)
        """
        logger.debug(f"🔍 Descobrindo tabelas MySQL: {user}@{host}:{port}/{database}")
        
        try:
            import pymysql
        except ImportError:
            return False, [], "Dependência pymysql não encontrada"
        
        try:
            # Configurar conexão
            connection_config = {
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'database': database,
                'charset': 'utf8mb4',
                'connect_timeout': 8,
                'read_timeout': 15,
                'write_timeout': 15,
                'autocommit': True
            }
            
            # Conectar
            connection = pymysql.connect(**connection_config)
            
            try:
                tables = []
                
                with connection.cursor() as cursor:
                    # Usar SHOW FULL TABLES para obter tipo da tabela
                    cursor.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
                    results = cursor.fetchall()
                    
                    for row in results:
                        table_name = row[0]
                        table_type = row[1] if len(row) > 1 else None
                        
                        tables.append(TableInfo(
                            name=table_name,
                            type=table_type
                        ))
                
                logger.debug(f"✅ Descobertas {len(tables)} tabelas MySQL")
                return True, tables, ""
                
            finally:
                connection.close()
                
        except Exception as e:
            logger.exception(f"❌ Erro ao descobrir tabelas MySQL: {e}")
            return False, [], f"Erro ao descobrir tabelas: {str(e)}"


class PostgreSQLValidator:
    """Validador de conexões PostgreSQL"""
    
    @staticmethod
    def validate_connection(host: str, port: int, database: str, user: str, password: str) -> ValidationResult:
        """
        Valida uma conexão PostgreSQL
        
        Args:
            host: Endereço do servidor
            port: Porta do servidor
            database: Nome do banco de dados
            user: Usuário
            password: Senha
            
        Returns:
            ValidationResult: Resultado da validação
        """
        logger.debug(f"🔍 Validando conexão PostgreSQL: {user}@{host}:{port}/{database}")
        
        try:
            import psycopg2
            from psycopg2 import OperationalError, DatabaseError
        except ImportError:
            logger.error("❌ psycopg2 não está instalado")
            return ValidationResult(
                success=False,
                message="Dependência psycopg2-binary não encontrada. Execute: pip install psycopg2-binary",
                error_details="ImportError: psycopg2"
            )
        
        start_time = time.time()
        
        try:
            # Configurar string de conexão
            connection_string = (
                f"host='{host}' port={port} dbname='{database}' "
                f"user='{user}' password='{password}' "
                f"connect_timeout=8"
            )
            
            # Tentar conectar
            connection = psycopg2.connect(connection_string)
            
            try:
                # Executar teste básico
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    
                    if result and result[0] == 1:
                        connection_time = int((time.time() - start_time) * 1000)
                        logger.debug(f"✅ Conexão PostgreSQL validada em {connection_time}ms")
                        
                        return ValidationResult(
                            success=True,
                            message="Conexão validada com sucesso",
                            connection_time_ms=connection_time
                        )
                    else:
                        return ValidationResult(
                            success=False,
                            message="Falha no teste de conectividade",
                            error_details="SELECT 1 retornou resultado inesperado"
                        )
                        
            finally:
                connection.close()
                
        except OperationalError as e:
            error_msg = str(e).strip()
            logger.warning(f"⚠️ Erro operacional PostgreSQL: {error_msg}")
            
            # Mapear erros comuns para mensagens amigáveis
            if "authentication failed" in error_msg.lower():
                message = "Acesso negado. Verifique usuário e senha."
            elif "database" in error_msg.lower() and "does not exist" in error_msg.lower():
                message = f"Banco de dados '{database}' não encontrado."
            elif "could not connect" in error_msg.lower() or "connection refused" in error_msg.lower():
                message = f"Não foi possível conectar ao servidor {host}:{port}."
            elif "timeout" in error_msg.lower():
                message = f"Timeout na conexão com {host}:{port}."
            else:
                message = "Não foi possível validar a conexão. Verifique os dados e tente novamente."
            
            return ValidationResult(
                success=False,
                message=message,
                error_details=f"PostgreSQL OperationalError: {error_msg}"
            )
            
        except DatabaseError as e:
            error_msg = str(e).strip()
            logger.warning(f"⚠️ Erro de banco PostgreSQL: {error_msg}")
            
            return ValidationResult(
                success=False,
                message="Erro no banco de dados. Verifique os dados e tente novamente.",
                error_details=f"PostgreSQL DatabaseError: {error_msg}"
            )
            
        except Exception as e:
            logger.exception(f"❌ Erro inesperado na validação PostgreSQL: {e}")
            return ValidationResult(
                success=False,
                message="Erro inesperado na validação. Verifique os dados e tente novamente.",
                error_details=str(e)
            )
    
    @staticmethod
    def discover_tables(host: str, port: int, database: str, user: str, password: str) -> Tuple[bool, List[TableInfo], str]:
        """
        Descobre tabelas disponíveis no banco PostgreSQL
        
        Args:
            host: Endereço do servidor
            port: Porta do servidor
            database: Nome do banco de dados
            user: Usuário
            password: Senha
            
        Returns:
            Tuple[bool, List[TableInfo], str]: (sucesso, lista de tabelas, mensagem de erro)
        """
        logger.debug(f"🔍 Descobrindo tabelas PostgreSQL: {user}@{host}:{port}/{database}")
        
        try:
            import psycopg2
        except ImportError:
            return False, [], "Dependência psycopg2-binary não encontrada"
        
        try:
            # Configurar string de conexão
            connection_string = (
                f"host='{host}' port={port} dbname='{database}' "
                f"user='{user}' password='{password}' "
                f"connect_timeout=8"
            )
            
            # Conectar
            connection = psycopg2.connect(connection_string)
            
            try:
                tables = []
                
                with connection.cursor() as cursor:
                    # Consultar tabelas do schema público
                    cursor.execute("""
                        SELECT table_name, table_type 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_type = 'BASE TABLE'
                        ORDER BY table_name
                    """)
                    
                    results = cursor.fetchall()
                    
                    for row in results:
                        table_name = row[0]
                        table_type = row[1] if len(row) > 1 else None
                        
                        tables.append(TableInfo(
                            name=table_name,
                            type=table_type
                        ))
                
                logger.debug(f"✅ Descobertas {len(tables)} tabelas PostgreSQL")
                return True, tables, ""
                
            finally:
                connection.close()
                
        except Exception as e:
            logger.exception(f"❌ Erro ao descobrir tabelas PostgreSQL: {e}")
            return False, [], f"Erro ao descobrir tabelas: {str(e)}"


class DatabaseValidatorFactory:
    """Factory para criar validadores de banco de dados"""
    
    @staticmethod
    def get_validator(db_type: str):
        """
        Obtém o validador apropriado para o tipo de banco
        
        Args:
            db_type: Tipo do banco ('mysql' ou 'postgresql')
            
        Returns:
            Classe do validador
            
        Raises:
            ValueError: Se o tipo não for suportado
        """
        if db_type.lower() == 'mysql':
            return MySQLValidator
        elif db_type.lower() == 'postgresql':
            return PostgreSQLValidator
        else:
            raise ValueError(f"Tipo de banco não suportado: {db_type}")
    
    @staticmethod
    def validate_connection(db_type: str, host: str, port: int, database: str, user: str, password: str) -> ValidationResult:
        """
        Valida uma conexão usando o validador apropriado
        
        Args:
            db_type: Tipo do banco ('mysql' ou 'postgresql')
            host: Endereço do servidor
            port: Porta do servidor
            database: Nome do banco de dados
            user: Usuário
            password: Senha
            
        Returns:
            ValidationResult: Resultado da validação
        """
        try:
            validator = DatabaseValidatorFactory.get_validator(db_type)
            return validator.validate_connection(host, port, database, user, password)
        except ValueError as e:
            return ValidationResult(
                success=False,
                message=str(e),
                error_details="Tipo de banco não suportado"
            )
    
    @staticmethod
    def discover_tables(db_type: str, host: str, port: int, database: str, user: str, password: str) -> Tuple[bool, List[TableInfo], str]:
        """
        Descobre tabelas usando o validador apropriado
        
        Args:
            db_type: Tipo do banco ('mysql' ou 'postgresql')
            host: Endereço do servidor
            port: Porta do servidor
            database: Nome do banco de dados
            user: Usuário
            password: Senha
            
        Returns:
            Tuple[bool, List[TableInfo], str]: (sucesso, lista de tabelas, mensagem de erro)
        """
        try:
            validator = DatabaseValidatorFactory.get_validator(db_type)
            return validator.discover_tables(host, port, database, user, password)
        except ValueError as e:
            return False, [], str(e)