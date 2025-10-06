"""
Gerenciamento de Fontes de Dados criptografadas
Suporta MySQL e PostgreSQL com criptografia AES-GCM
"""

import json
import os
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, asdict

from core.crypto import encrypt_data_to_file, decrypt_data_from_file
from core.paths import get_bridge_config_dir
from core.logger import logger


@dataclass
class DatabaseConnection:
    """Representa uma conexão de banco de dados"""
    host: str
    port: int
    database: str
    user: str
    password: str
    options: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.options is None:
            self.options = {}


@dataclass
class TableSelection:
    """Representa a seleção de tabelas de uma fonte"""
    selected: List[str]
    last_discovery_at: Optional[str] = None
    
    def __post_init__(self):
        if self.selected is None:
            self.selected = []


@dataclass
class DataSource:
    """Representa uma fonte de dados"""
    id: str
    type: str  # 'mysql' ou 'postgresql'
    name: str
    created_at: str
    conn: DatabaseConnection
    tables: TableSelection = None
    
    def __post_init__(self):
        if self.tables is None:
            self.tables = TableSelection([])
    
    def get_connection_summary(self) -> str:
        """
        Retorna um resumo da conexão no formato host:porta/database
        
        Returns:
            str: Resumo da conexão
        """
        return f"{self.conn.host}:{self.conn.port}/{self.conn.database}"
    
    def get_masked_password(self) -> str:
        """
        Retorna a senha mascarada para exibição
        
        Returns:
            str: Senha mascarada
        """
        if not self.conn.password:
            return "(vazia)"
        if len(self.conn.password) <= 4:
            return "•" * len(self.conn.password)
        return "•" * (len(self.conn.password) - 4) + self.conn.password[-4:]
    
    def get_formatted_created_at(self) -> str:
        """
        Retorna a data de criação formatada
        
        Returns:
            str: Data formatada no formato "YYYY-MM-DD HH:MM"
        """
        try:
            dt = datetime.fromisoformat(self.created_at.replace('Z', '+00:00'))
            return dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            return self.created_at


class DataSourcesStore:
    """Gerenciador de fontes de dados criptografadas"""
    
    def __init__(self):
        """Inicializa o store de fontes de dados"""
        self.datasources: List[DataSource] = []
        self.load()
    
    def get_datasources_file_path(self) -> str:
        """
        Obtém o caminho do arquivo de fontes de dados
        
        Returns:
            str: Caminho do arquivo datasources.enc
        """
        bridge_dir = get_bridge_config_dir()
        return os.path.join(bridge_dir, "datasources.enc")
    
    def load(self) -> List[DataSource]:
        """
        Carrega as fontes de dados do arquivo criptografado ou retorna lista vazia
        
        Returns:
            Lista de datasources carregadas
            
        Raises:
            Exception: Se houver erro ao carregar
        """
        logger.debug("📂 Carregando fontes de dados...")
        try:
            datasources_path = self.get_datasources_file_path()
            
            # Verificar se arquivo existe
            if not os.path.exists(datasources_path):
                logger.debug("📄 Arquivo de fontes de dados não existe, iniciando vazio")
                self.datasources = []
                return self.datasources
            
            # Tentar carregar e descriptografar dados
            try:
                data = decrypt_data_from_file(datasources_path)
            except Exception as decrypt_error:
                logger.warning(f"⚠️ Não foi possível descriptografar datasources: {decrypt_error}")
                logger.debug("📄 Retornando lista vazia de datasources")
                self.datasources = []
                return self.datasources
            
            # Processar dados carregados
            if "sources" in data and isinstance(data["sources"], list):
                self.datasources = []
                for source_data in data["sources"]:
                    try:
                        # Reconstruir objetos DataSource
                        conn_data = source_data["conn"]
                        conn = DatabaseConnection(
                            host=conn_data["host"],
                            port=conn_data["port"],
                            database=conn_data["database"],
                            user=conn_data["user"],
                            password=conn_data["password"],
                            options=conn_data.get("options", {})
                        )
                        
                        tables_data = source_data.get("tables", {"selected": []})
                        tables = TableSelection(
                            selected=tables_data.get("selected", []),
                            last_discovery_at=tables_data.get("last_discovery_at")
                        )
                        
                        datasource = DataSource(
                            id=source_data["id"],
                            type=source_data["type"],
                            name=source_data["name"],
                            created_at=source_data["created_at"],
                            conn=conn,
                            tables=tables
                        )
                        
                        self.datasources.append(datasource)
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Erro ao carregar fonte de dados: {e}")
                        continue
                
                logger.debug(f"✅ {len(self.datasources)} fontes de dados carregadas")
            else:
                logger.warning("⚠️ Formato de arquivo inválido, iniciando vazio")
                self.datasources = []
                
            return self.datasources
                
        except Exception as e:
            logger.error(f"❌ Erro ao carregar fontes de dados: {e}")
            self.datasources = []
            return self.datasources
    
    def save(self) -> None:
        """
        Salva as fontes de dados criptografadas no arquivo
        
        Raises:
            Exception: Se houver erro ao salvar
        """
        logger.debug("💾 Salvando fontes de dados...")
        try:
            # Preparar dados para salvar
            sources_data = []
            for datasource in self.datasources:
                source_dict = {
                    "id": datasource.id,
                    "type": datasource.type,
                    "name": datasource.name,
                    "created_at": datasource.created_at,
                    "conn": {
                        "host": datasource.conn.host,
                        "port": datasource.conn.port,
                        "database": datasource.conn.database,
                        "user": datasource.conn.user,
                        "password": datasource.conn.password,
                        "options": datasource.conn.options
                    },
                    "tables": {
                        "selected": datasource.tables.selected,
                        "last_discovery_at": datasource.tables.last_discovery_at
                    }
                }
                sources_data.append(source_dict)
            
            data = {
                "version": 1,
                "sources": sources_data
            }
            
            # Obter caminho do arquivo
            datasources_path = self.get_datasources_file_path()
            
            # Salvar dados criptografados
            encrypt_data_to_file(data, datasources_path)
            
            logger.debug(f"✅ Fontes de dados salvas com sucesso em {datasources_path}")
            
        except Exception as e:
            logger.exception(f"❌ Erro ao salvar fontes de dados: {e}")
            raise Exception(f"Erro ao salvar fontes de dados: {e}")
    
    def add_datasource(self, name: str, db_type: str, conn: DatabaseConnection) -> DataSource:
        """
        Adiciona uma nova fonte de dados
        
        Args:
            name: Nome da fonte de dados
            db_type: Tipo do banco ('mysql' ou 'postgresql')
            conn: Dados de conexão
            
        Returns:
            DataSource: A fonte de dados criada
            
        Raises:
            ValueError: Se o nome já existir
            Exception: Se houver erro ao salvar
        """
        logger.debug(f"➕ Adicionando fonte de dados: {name} ({db_type})")
        
        # Verificar se nome já existe
        if self.get_datasource_by_name(name):
            raise ValueError(f"Já existe uma fonte de dados com o nome '{name}'")
        
        # Criar nova fonte de dados
        datasource = DataSource(
            id=str(uuid.uuid4()),
            type=db_type,
            name=name,
            created_at=datetime.utcnow().isoformat() + "Z",
            conn=conn,
            tables=TableSelection([])
        )
        
        # Adicionar à lista
        self.datasources.append(datasource)
        
        # Salvar
        self.save()
        
        logger.debug(f"✅ Fonte de dados '{name}' adicionada com sucesso")
        return datasource
    
    def delete_datasource(self, name: str) -> bool:
        """
        Remove uma fonte de dados pelo nome
        
        Args:
            name: Nome da fonte de dados
            
        Returns:
            bool: True se removida, False se não encontrada
            
        Raises:
            Exception: Se houver erro ao salvar
        """
        logger.debug(f"🗑️ Removendo fonte de dados: {name}")
        
        # Encontrar e remover
        for i, datasource in enumerate(self.datasources):
            if datasource.name == name:
                removed = self.datasources.pop(i)
                self.save()
                logger.debug(f"✅ Fonte de dados '{name}' removida com sucesso")
                return True
        
        logger.warning(f"⚠️ Fonte de dados '{name}' não encontrada")
        return False
    
    def list_datasources(self) -> List[DataSource]:
        """
        Lista todas as fontes de dados
        
        Returns:
            List[DataSource]: Lista de fontes de dados
        """
        return self.datasources.copy()
    
    def get_datasource_by_name(self, name: str) -> Optional[DataSource]:
        """
        Busca uma fonte de dados pelo nome
        
        Args:
            name: Nome da fonte de dados
            
        Returns:
            Optional[DataSource]: A fonte de dados ou None se não encontrada
        """
        for datasource in self.datasources:
            if datasource.name == name:
                return datasource
        return None
    
    def get_datasource_by_id(self, datasource_id: str) -> Optional[DataSource]:
        """
        Busca uma fonte de dados pelo ID
        
        Args:
            datasource_id: ID da fonte de dados
            
        Returns:
            Optional[DataSource]: A fonte de dados ou None se não encontrada
        """
        for datasource in self.datasources:
            if datasource.id == datasource_id:
                return datasource
        return None
    
    def save_selected_tables(self, name: str, selected_tables: List[str]) -> bool:
        """
        Salva as tabelas selecionadas para uma fonte de dados
        
        Args:
            name: Nome da fonte de dados
            selected_tables: Lista de tabelas selecionadas
            
        Returns:
            bool: True se salvo com sucesso, False se fonte não encontrada
            
        Raises:
            Exception: Se houver erro ao salvar
        """
        logger.debug(f"💾 Salvando tabelas selecionadas para '{name}': {selected_tables}")
        
        datasource = self.get_datasource_by_name(name)
        if not datasource:
            logger.warning(f"⚠️ Fonte de dados '{name}' não encontrada")
            return False
        
        # Atualizar seleção de tabelas
        datasource.tables.selected = selected_tables
        datasource.tables.last_discovery_at = datetime.utcnow().isoformat() + "Z"
        
        # Salvar
        self.save()
        
        logger.debug(f"✅ Tabelas salvas para '{name}': {len(selected_tables)} tabelas")
        return True
    
    def get_datasources_count(self) -> int:
        """
        Retorna o número de fontes de dados cadastradas
        
        Returns:
            int: Número de fontes de dados
        """
        return len(self.datasources)


# Instância global do store
datasources_store = DataSourcesStore()