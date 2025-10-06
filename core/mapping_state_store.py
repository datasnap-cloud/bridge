"""
Gerenciador de estado dos mapeamentos para o pipeline de sincronização.

Este módulo é responsável por manter o estado de cada mapeamento,
incluindo timestamps de última sincronização, contadores de registros
e informações de erro.
"""

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict

from .timeutil import get_current_timestamp, timestamp_to_iso
from .paths import get_default_paths


@dataclass
class MappingState:
    """
    Estado de um mapeamento específico.
    """
    mapping_name: str
    last_sync_timestamp: Optional[int] = None
    last_sync_iso: Optional[str] = None
    total_records_processed: int = 0
    last_batch_records: int = 0
    last_error: Optional[str] = None
    last_error_timestamp: Optional[int] = None
    sync_count: int = 0
    is_running: bool = False
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    
    def __post_init__(self):
        """Inicializa campos automáticos após criação."""
        current_time = get_current_timestamp()
        if self.created_at is None:
            self.created_at = current_time
        self.updated_at = current_time
    
    def update_sync_success(self, records_processed: int) -> None:
        """
        Atualiza o estado após uma sincronização bem-sucedida.
        
        Args:
            records_processed: Número de registros processados
        """
        current_time = get_current_timestamp()
        self.last_sync_timestamp = current_time
        self.last_sync_iso = timestamp_to_iso(current_time)
        self.last_batch_records = records_processed
        self.total_records_processed += records_processed
        self.sync_count += 1
        self.last_error = None
        self.last_error_timestamp = None
        self.is_running = False
        self.updated_at = current_time
    
    def update_sync_error(self, error_message: str) -> None:
        """
        Atualiza o estado após um erro na sincronização.
        
        Args:
            error_message: Mensagem de erro
        """
        current_time = get_current_timestamp()
        self.last_error = error_message
        self.last_error_timestamp = current_time
        self.is_running = False
        self.updated_at = current_time
    
    def start_sync(self) -> None:
        """Marca o início de uma sincronização."""
        self.is_running = True
        self.updated_at = get_current_timestamp()
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte o estado para dicionário."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MappingState':
        """Cria uma instância a partir de um dicionário."""
        return cls(**data)


class MappingStateStore:
    """
    Armazena e gerencia o estado de todos os mapeamentos.
    """
    
    def __init__(self, state_file: Optional[Path] = None):
        """
        Inicializa o store de estados.
        
        Args:
            state_file: Caminho para o arquivo de estado (opcional)
        """
        if state_file is None:
            paths = get_default_paths()
            paths.ensure_directories()
            state_file = paths.sync_state_file
        
        self.state_file = state_file
        self._states: Dict[str, MappingState] = {}
        self._lock = threading.Lock()
        
        # Carrega estados existentes
        self._load_states()
    
    def _load_states(self) -> None:
        """Carrega os estados do arquivo."""
        if not self.state_file.exists():
            return
        
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            for mapping_name, state_data in data.items():
                self._states[mapping_name] = MappingState.from_dict(state_data)
        
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"Erro ao carregar estados: {e}")
            # Em caso de erro, inicia com estados vazios
            self._states = {}
    
    def _save_states(self) -> None:
        """Salva os estados no arquivo."""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"[DEBUG] _save_states iniciado")
        
        # Garante que o diretório pai existe
        logger.info(f"[DEBUG] Criando diretório pai: {self.state_file.parent}")
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"[DEBUG] Diretório pai criado com sucesso")
        
        try:
            logger.info(f"[DEBUG] Convertendo estados para dict...")
            data = {name: state.to_dict() for name, state in self._states.items()}
            logger.info(f"[DEBUG] Estados convertidos: {len(data)} itens")
            
            logger.info(f"[DEBUG] Abrindo arquivo para escrita: {self.state_file}")
            with open(self.state_file, 'w', encoding='utf-8') as f:
                logger.info(f"[DEBUG] Arquivo aberto, salvando JSON...")
                json.dump(data, f, indent=2, ensure_ascii=False)
                logger.info(f"[DEBUG] JSON salvo com sucesso")
        
        except Exception as e:
            logger.error(f"[DEBUG] Erro ao salvar estados: {e}")
            print(f"Erro ao salvar estados: {e}")
        
        logger.info(f"[DEBUG] _save_states concluído")
    
    def _get_state_unsafe(self, mapping_name: str) -> MappingState:
        """
        Obtém o estado de um mapeamento sem adquirir lock (uso interno).
        
        Args:
            mapping_name: Nome do mapeamento
            
        Returns:
            MappingState: Estado do mapeamento
        """
        if mapping_name not in self._states:
            self._states[mapping_name] = MappingState(mapping_name=mapping_name)
            self._save_states()
        
        return self._states[mapping_name]
    
    def get_state(self, mapping_name: str) -> MappingState:
        """
        Obtém o estado de um mapeamento.
        
        Args:
            mapping_name: Nome do mapeamento
            
        Returns:
            MappingState: Estado do mapeamento
        """
        with self._lock:
            return self._get_state_unsafe(mapping_name)
    
    def update_state(self, mapping_name: str, state: MappingState) -> None:
        """
        Atualiza o estado de um mapeamento.
        
        Args:
            mapping_name: Nome do mapeamento
            state: Novo estado
        """
        with self._lock:
            self._states[mapping_name] = state
            self._save_states()
    
    def start_sync(self, mapping_name: str) -> None:
        """
        Marca o início de uma sincronização.
        
        Args:
            mapping_name: Nome do mapeamento
        """
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"[DEBUG] MappingStateStore.start_sync iniciado para: {mapping_name}")
        logger.info(f"[DEBUG] Tentando adquirir lock...")
        
        with self._lock:
            logger.info(f"[DEBUG] Lock adquirido, chamando _get_state_unsafe...")
            state = self._get_state_unsafe(mapping_name)
            logger.info(f"[DEBUG] _get_state_unsafe retornou, chamando state.start_sync...")
            state.start_sync()
            logger.info(f"[DEBUG] state.start_sync concluído, chamando _save_states...")
            self._save_states()
            logger.info(f"[DEBUG] _save_states concluído")
        
        logger.info(f"[DEBUG] MappingStateStore.start_sync concluído")
    
    def finish_sync_success(self, mapping_name: str, records_processed: int) -> None:
        """
        Marca o fim bem-sucedido de uma sincronização.
        
        Args:
            mapping_name: Nome do mapeamento
            records_processed: Número de registros processados
        """
        with self._lock:
            state = self._get_state_unsafe(mapping_name)
            state.update_sync_success(records_processed)
            self._save_states()
    
    def finish_sync_error(self, mapping_name: str, error_message: str) -> None:
        """
        Marca o fim com erro de uma sincronização.
        
        Args:
            mapping_name: Nome do mapeamento
            error_message: Mensagem de erro
        """
        with self._lock:
            state = self._get_state_unsafe(mapping_name)
            state.update_sync_error(error_message)
            self._save_states()
    
    def get_all_states(self) -> Dict[str, MappingState]:
        """
        Retorna todos os estados de mapeamentos.
        
        Returns:
            Dict[str, MappingState]: Dicionário com todos os estados
        """
        with self._lock:
            return self._states.copy()
    
    def get_running_mappings(self) -> List[str]:
        """
        Retorna lista de mapeamentos que estão executando.
        
        Returns:
            List[str]: Lista de nomes de mapeamentos em execução
        """
        with self._lock:
            return [name for name, state in self._states.items() if state.is_running]
    
    def is_mapping_running(self, mapping_name: str) -> bool:
        """
        Verifica se um mapeamento está em execução.
        
        Args:
            mapping_name: Nome do mapeamento
            
        Returns:
            bool: True se o mapeamento está executando
        """
        state = self.get_state(mapping_name)
        return state.is_running
    
    def get_last_sync_timestamp(self, mapping_name: str) -> Optional[int]:
        """
        Retorna o timestamp da última sincronização.
        
        Args:
            mapping_name: Nome do mapeamento
            
        Returns:
            Optional[int]: Timestamp da última sincronização ou None
        """
        state = self.get_state(mapping_name)
        return state.last_sync_timestamp
    
    def clear_state(self, mapping_name: str) -> None:
        """
        Remove o estado de um mapeamento.
        
        Args:
            mapping_name: Nome do mapeamento
        """
        with self._lock:
            if mapping_name in self._states:
                del self._states[mapping_name]
                self._save_states()
    
    def clear_all_states(self) -> None:
        """Remove todos os estados."""
        with self._lock:
            self._states.clear()
            self._save_states()
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Retorna um resumo dos estados.
        
        Returns:
            Dict[str, Any]: Resumo com estatísticas
        """
        with self._lock:
            total_mappings = len(self._states)
            running_mappings = len([s for s in self._states.values() if s.is_running])
            total_records = sum(s.total_records_processed for s in self._states.values())
            total_syncs = sum(s.sync_count for s in self._states.values())
            mappings_with_errors = len([s for s in self._states.values() if s.last_error])
            
            return {
                'total_mappings': total_mappings,
                'running_mappings': running_mappings,
                'total_records_processed': total_records,
                'total_syncs': total_syncs,
                'mappings_with_errors': mappings_with_errors,
                'last_updated': get_current_timestamp()
            }


# Instância global do store
_global_store: Optional[MappingStateStore] = None


def get_global_state_store() -> MappingStateStore:
    """
    Retorna a instância global do store de estados.
    
    Returns:
        MappingStateStore: Instância global
    """
    global _global_store
    if _global_store is None:
        _global_store = MappingStateStore()
    return _global_store


def reset_global_state_store() -> None:
    """Reseta a instância global do store."""
    global _global_store
    _global_store = None