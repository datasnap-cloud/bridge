"""
Módulo para gerenciar caminhos e estrutura de diretórios do Bridge
"""

import os
import sys
import shutil
from pathlib import Path
from typing import Union, List, Optional


def get_app_root_dir() -> Path:
    """
    Resolve o diretório base ao lado do app (não usar home do usuário).
    
    Returns:
        Path: Caminho para o diretório raiz da aplicação
    """
    if getattr(sys, 'frozen', False):
        # Se executando como executável empacotado (PyInstaller, etc.)
        app_dir = Path(sys.executable).parent
    else:
        # Se executando como script Python
        app_dir = Path(__file__).parent.parent
    
    return app_dir


def get_bridge_config_dir() -> Path:
    """
    Retorna o diretório de configuração .bridge/
    
    Returns:
        Path: Caminho para o diretório .bridge/
    """
    return get_app_root_dir() / ".bridge"


def get_config_file_path() -> Path:
    """
    Retorna o caminho para o arquivo config.json
    
    Returns:
        Path: Caminho para config.json
    """
    return get_bridge_config_dir() / "config.json"


def get_api_keys_file_path() -> Path:
    """
    Retorna o caminho para o arquivo api_keys.enc
    
    Returns:
        Path: Caminho para api_keys.enc
    """
    return get_bridge_config_dir() / "api_keys.enc"


def ensure_bridge_directory() -> None:
    """
    Cria o diretório .bridge/ se não existir, com permissões estritas
    """
    bridge_dir = get_bridge_config_dir()
    
    if not bridge_dir.exists():
        bridge_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        
        # Tentar definir permissões estritas (ignorar gracefully no Windows)
        try:
            os.chmod(bridge_dir, 0o700)
        except (OSError, AttributeError):
            # Windows ou sistema que não suporta chmod
            pass


def set_secure_file_permissions(file_path: Union[str, Path]) -> None:
    """
    Define permissões seguras para um arquivo (0o600)
    
    Args:
        file_path: Caminho para o arquivo
    """
    try:
        os.chmod(file_path, 0o600)
    except (OSError, AttributeError):
        # Windows ou sistema que não suporta chmod - ignorar gracefully
        pass


class BridgePaths:
    """
    Classe para gerenciar todos os caminhos utilizados pelo DataSnap Bridge.
    """
    
    def __init__(self, base_dir: Optional[str] = None):
        """
        Inicializa os caminhos do bridge.
        
        Args:
            base_dir: Diretório base do projeto (padrão: diretório atual)
        """
        if base_dir is None:
            self.base_dir = get_app_root_dir()
        else:
            self.base_dir = Path(base_dir).resolve()
        
        self.bridge_dir = self.base_dir / ".bridge"
        
        # Diretórios principais
        self.config_dir = self.bridge_dir / "config"
        self.mappings_dir = self.config_dir / "mappings"
        self.logs_dir = self.bridge_dir / "logs"
        self.tmp_dir = self.bridge_dir / "tmp"
        self.uploads_dir = self.tmp_dir / "uploads"
        self.cache_dir = self.bridge_dir / "cache"
        
        # Arquivos de configuração
        self.api_keys_file = self.config_dir / "api_keys.json"
        self.data_sources_file = self.config_dir / "data_sources.json"
        
        # Arquivos de estado
        self.state_dir = self.bridge_dir / "state"
        self.sync_state_file = self.state_dir / "sync_state.json"
        
    def ensure_directories(self) -> None:
        """
        Garante que todos os diretórios necessários existam.
        """
        directories = [
            self.bridge_dir,
            self.config_dir,
            self.mappings_dir,
            self.logs_dir,
            self.tmp_dir,
            self.uploads_dir,
            self.cache_dir,
            self.state_dir
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_mapping_file(self, mapping_name: str) -> Path:
        """
        Retorna o caminho para um arquivo de mapeamento específico.
        
        Args:
            mapping_name: Nome do mapeamento
            
        Returns:
            Path: Caminho para o arquivo de mapeamento
        """
        return self.mappings_dir / f"{mapping_name}.json"
    
    def get_log_file(self, log_name: str) -> Path:
        """
        Retorna o caminho para um arquivo de log específico.
        
        Args:
            log_name: Nome do arquivo de log
            
        Returns:
            Path: Caminho para o arquivo de log
        """
        return self.logs_dir / f"{log_name}.log"
    
    def get_upload_file(self, filename: str) -> Path:
        """
        Retorna o caminho para um arquivo na pasta de uploads.
        
        Args:
            filename: Nome do arquivo
            
        Returns:
            Path: Caminho para o arquivo de upload
        """
        return self.uploads_dir / filename
    
    def get_cache_file(self, cache_name: str) -> Path:
        """
        Retorna o caminho para um arquivo de cache específico.
        
        Args:
            cache_name: Nome do arquivo de cache
            
        Returns:
            Path: Caminho para o arquivo de cache
        """
        return self.cache_dir / f"{cache_name}.json"
    
    def list_mapping_files(self) -> List[Path]:
        """
        Lista todos os arquivos de mapeamento disponíveis.
        
        Returns:
            List[Path]: Lista de caminhos para arquivos de mapeamento
        """
        if not self.mappings_dir.exists():
            return []
        
        return list(self.mappings_dir.glob("*.json"))
    
    def clean_tmp_directory(self) -> None:
        """
        Limpa o diretório temporário, removendo todos os arquivos.
        """
        if self.tmp_dir.exists():
            shutil.rmtree(self.tmp_dir)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
    
    def clean_uploads_directory(self) -> None:
        """
        Limpa apenas o diretório de uploads.
        """
        if self.uploads_dir.exists():
            shutil.rmtree(self.uploads_dir)
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
    
    def get_relative_path(self, path: Path) -> str:
        """
        Retorna o caminho relativo ao diretório base do projeto.
        
        Args:
            path: Caminho absoluto
            
        Returns:
            str: Caminho relativo
        """
        try:
            return str(path.relative_to(self.base_dir))
        except ValueError:
            return str(path)
    
    def file_exists(self, path: Path) -> bool:
        """
        Verifica se um arquivo existe.
        
        Args:
            path: Caminho para o arquivo
            
        Returns:
            bool: True se o arquivo existe
        """
        return path.exists() and path.is_file()
    
    def directory_exists(self, path: Path) -> bool:
        """
        Verifica se um diretório existe.
        
        Args:
            path: Caminho para o diretório
            
        Returns:
            bool: True se o diretório existe
        """
        return path.exists() and path.is_dir()
    
    def get_file_size(self, path: Path) -> int:
        """
        Retorna o tamanho de um arquivo em bytes.
        
        Args:
            path: Caminho para o arquivo
            
        Returns:
            int: Tamanho do arquivo em bytes
        """
        if not self.file_exists(path):
            return 0
        return path.stat().st_size
    
    def get_directory_size(self, path: Path) -> int:
        """
        Retorna o tamanho total de um diretório em bytes.
        
        Args:
            path: Caminho para o diretório
            
        Returns:
            int: Tamanho total em bytes
        """
        if not self.directory_exists(path):
            return 0
        
        total_size = 0
        for file_path in path.rglob("*"):
            if file_path.is_file():
                total_size += file_path.stat().st_size
        
        return total_size


def get_default_paths() -> BridgePaths:
    """
    Retorna uma instância de BridgePaths com os caminhos padrão.
    
    Returns:
        BridgePaths: Instância configurada com caminhos padrão
    """
    return BridgePaths()


def ensure_file_directory(file_path: Path) -> None:
    """
    Garante que o diretório pai de um arquivo existe.
    
    Args:
        file_path: Caminho para o arquivo
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)


def safe_filename(filename: str) -> str:
    """
    Converte um nome de arquivo para um formato seguro.
    
    Args:
        filename: Nome do arquivo original
        
    Returns:
        str: Nome do arquivo seguro
    """
    # Remove caracteres perigosos
    unsafe_chars = '<>:"/\\|?*'
    safe_name = filename
    
    for char in unsafe_chars:
        safe_name = safe_name.replace(char, '_')
    
    # Remove espaços extras e pontos no final
    safe_name = safe_name.strip('. ')
    
    # Garante que não está vazio
    if not safe_name:
        safe_name = "unnamed_file"
    
    return safe_name


def format_file_size(size_bytes: int) -> str:
    """
    Formata um tamanho de arquivo em bytes para formato legível.
    
    Args:
        size_bytes: Tamanho em bytes
        
    Returns:
        str: Tamanho formatado (ex: "1.5 MB", "2.3 GB")
    """
    if size_bytes == 0:
        return "0 B"
    
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    size = float(size_bytes)
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.1f} {units[unit_index]}"