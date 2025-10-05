"""
Módulo de logging configurável para DataSnap Bridge.
Permite ativar debug via variável de ambiente BRIDGE_DEBUG.
"""

import logging
import os
from pathlib import Path
from typing import Optional


class BridgeLogger:
    """Logger configurável para o DataSnap Bridge."""
    
    _instance: Optional['BridgeLogger'] = None
    _logger: Optional[logging.Logger] = None
    
    def __new__(cls) -> 'BridgeLogger':
        """Implementa padrão Singleton."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Inicializa o logger se ainda não foi configurado."""
        if self._logger is None:
            self._setup_logger()
    
    def _setup_logger(self) -> None:
        """Configura o logger baseado na variável de ambiente."""
        # Verifica se debug está ativado
        debug_enabled = os.getenv('BRIDGE_DEBUG', '').lower() in ('1', 'true', 'yes', 'on')
        
        # Configura o logger
        self._logger = logging.getLogger('datasnap_bridge')
        self._logger.setLevel(logging.DEBUG if debug_enabled else logging.INFO)
        
        # Remove handlers existentes para evitar duplicação
        for handler in self._logger.handlers[:]:
            self._logger.removeHandler(handler)
        
        if debug_enabled:
            # Configura handler para arquivo
            log_file = Path(__file__).parent.parent / 'bridge.log'
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            
            # Formato detalhado para debug
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s() - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            self._logger.addHandler(file_handler)
            
            # Também loga no console em modo debug
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            console_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%H:%M:%S'
            )
            console_handler.setFormatter(console_formatter)
            self._logger.addHandler(console_handler)
            
            self._logger.debug("🐛 Debug mode ativado - Logging para arquivo bridge.log")
        else:
            # Em modo normal, apenas erros críticos no console
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.ERROR)
            console_formatter = logging.Formatter('%(levelname)s: %(message)s')
            console_handler.setFormatter(console_formatter)
            self._logger.addHandler(console_handler)
    
    def debug(self, message: str, *args, **kwargs) -> None:
        """Log de debug."""
        if self._logger:
            self._logger.debug(message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs) -> None:
        """Log de informação."""
        if self._logger:
            self._logger.info(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs) -> None:
        """Log de aviso."""
        if self._logger:
            self._logger.warning(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs) -> None:
        """Log de erro."""
        if self._logger:
            self._logger.error(message, *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs) -> None:
        """Log crítico."""
        if self._logger:
            self._logger.critical(message, *args, **kwargs)
    
    def exception(self, message: str, *args, **kwargs) -> None:
        """Log de exceção com traceback."""
        if self._logger:
            self._logger.exception(message, *args, **kwargs)
    
    @property
    def is_debug_enabled(self) -> bool:
        """Verifica se o debug está ativado."""
        return self._logger and self._logger.level == logging.DEBUG


# Instância global do logger
logger = BridgeLogger()