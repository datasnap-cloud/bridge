"""
Módulo para coleta e gerenciamento de métricas do pipeline de sincronização.
Coleta métricas de performance, erros e estatísticas de uso.
"""

import json
import logging
import time
import psutil
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
from threading import Lock
import statistics

from core.timeutil import get_current_timestamp, format_duration
from core.paths import BridgePaths


logger = logging.getLogger(__name__)


@dataclass
class SyncMetrics:
    """Métricas de uma sincronização."""
    mapping_name: str
    schema_slug: str
    start_time: int
    end_time: Optional[int] = None
    duration: Optional[int] = None
    
    # Métricas de dados
    records_extracted: int = 0
    records_written: int = 0
    records_uploaded: int = 0
    records_deleted: int = 0
    
    # Métricas de arquivos
    files_created: int = 0
    files_uploaded: int = 0
    total_file_size: int = 0
    
    # Métricas de performance
    extraction_time: Optional[int] = None
    writing_time: Optional[int] = None
    upload_time: Optional[int] = None
    
    # Métricas de sistema
    peak_memory_mb: Optional[float] = None
    avg_cpu_percent: Optional[float] = None
    
    # Status e erros
    success: bool = False
    error_message: Optional[str] = None
    retry_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        return asdict(self)
    
    def calculate_rates(self) -> Dict[str, float]:
        """Calcula taxas de processamento."""
        if not self.duration or self.duration == 0:
            return {}
        
        return {
            'records_per_second': self.records_extracted / self.duration,
            'mb_per_second': (self.total_file_size / (1024 * 1024)) / self.duration,
            'files_per_minute': (self.files_created / self.duration) * 60
        }


@dataclass
class SystemMetrics:
    """Métricas do sistema."""
    timestamp: int
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    disk_usage_percent: float
    disk_free_gb: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        return asdict(self)


class MetricsCollector:
    """Coletor de métricas em tempo real."""
    
    def __init__(self, collection_interval: int = 5):
        """
        Inicializa o coletor.
        
        Args:
            collection_interval: Intervalo de coleta em segundos
        """
        self.collection_interval = collection_interval
        self.system_metrics: deque = deque(maxlen=1000)  # Últimas 1000 amostras
        self.sync_metrics: List[SyncMetrics] = []
        self.current_sync: Optional[SyncMetrics] = None
        
        self._lock = Lock()
        self._collecting = False
        
        # Métricas agregadas
        self.daily_stats = defaultdict(lambda: {
            'syncs_count': 0,
            'total_records': 0,
            'total_files': 0,
            'total_size': 0,
            'success_count': 0,
            'error_count': 0
        })
    
    def start_sync_metrics(self, mapping_name: str, schema_slug: str) -> SyncMetrics:
        """
        Inicia coleta de métricas para uma sincronização.
        
        Args:
            mapping_name: Nome do mapeamento
            schema_slug: Slug do schema
            
        Returns:
            Instância de métricas da sincronização
        """
        with self._lock:
            self.current_sync = SyncMetrics(
                mapping_name=mapping_name,
                schema_slug=schema_slug,
                start_time=get_current_timestamp()
            )
            
            logger.debug(f"Iniciando coleta de métricas: {mapping_name}")
            return self.current_sync
    
    def finish_sync_metrics(self, success: bool = True, error_message: str = None) -> Optional[SyncMetrics]:
        """
        Finaliza coleta de métricas para uma sincronização.
        
        Args:
            success: Se a sincronização foi bem-sucedida
            error_message: Mensagem de erro (se houver)
            
        Returns:
            Métricas finalizadas ou None
        """
        with self._lock:
            if not self.current_sync:
                return None
            
            # Finaliza métricas
            self.current_sync.end_time = get_current_timestamp()
            self.current_sync.duration = self.current_sync.end_time - self.current_sync.start_time
            self.current_sync.success = success
            self.current_sync.error_message = error_message
            
            # Calcula métricas de sistema
            if self.system_metrics:
                recent_metrics = list(self.system_metrics)[-10:]  # Últimas 10 amostras
                self.current_sync.peak_memory_mb = max(m.memory_used_mb for m in recent_metrics)
                self.current_sync.avg_cpu_percent = statistics.mean(m.cpu_percent for m in recent_metrics)
            
            # Adiciona às métricas coletadas
            completed_sync = self.current_sync
            self.sync_metrics.append(completed_sync)
            
            # Atualiza estatísticas diárias
            self._update_daily_stats(completed_sync)
            
            self.current_sync = None
            
            logger.debug(f"Métricas finalizadas: {completed_sync.mapping_name}")
            return completed_sync
    
    def update_extraction_metrics(self, records_count: int, duration: int) -> None:
        """
        Atualiza métricas de extração.
        
        Args:
            records_count: Número de registros extraídos
            duration: Duração da extração
        """
        with self._lock:
            if self.current_sync:
                self.current_sync.records_extracted = records_count
                self.current_sync.extraction_time = duration
    
    def update_writing_metrics(self, records_count: int, files_count: int, 
                              total_size: int, duration: int) -> None:
        """
        Atualiza métricas de escrita.
        
        Args:
            records_count: Número de registros escritos
            files_count: Número de arquivos criados
            total_size: Tamanho total dos arquivos
            duration: Duração da escrita
        """
        with self._lock:
            if self.current_sync:
                self.current_sync.records_written = records_count
                self.current_sync.files_created = files_count
                self.current_sync.total_file_size = total_size
                self.current_sync.writing_time = duration
    
    def update_upload_metrics(self, files_uploaded: int, records_uploaded: int, 
                             duration: int, retry_count: int = 0) -> None:
        """
        Atualiza métricas de upload.
        
        Args:
            files_uploaded: Número de arquivos enviados
            records_uploaded: Número de registros enviados
            duration: Duração do upload
            retry_count: Número de tentativas
        """
        with self._lock:
            if self.current_sync:
                self.current_sync.files_uploaded = files_uploaded
                self.current_sync.records_uploaded = records_uploaded
                self.current_sync.upload_time = duration
                self.current_sync.retry_count = retry_count
    
    def update_deletion_metrics(self, records_deleted: int, mapping_name: str) -> None:
        """
        Atualiza métricas de deleção após upload.
        
        Args:
            records_deleted: Número de registros deletados
            mapping_name: Nome do mapeamento
        """
        with self._lock:
            if self.current_sync:
                # Adiciona informação de deleção às métricas atuais
                if not hasattr(self.current_sync, 'records_deleted'):
                    self.current_sync.records_deleted = 0
                self.current_sync.records_deleted = records_deleted
                
                logger.info(f"📊 Métricas de deleção atualizadas: {records_deleted} registros deletados para {mapping_name}")
    
    def collect_system_metrics(self) -> SystemMetrics:
        """
        Coleta métricas do sistema.
        
        Returns:
            Métricas do sistema atual
        """
        try:
            # CPU e memória
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            # Disco (diretório do projeto)
            paths = BridgePaths()
            disk_usage = psutil.disk_usage(str(paths.get_app_root()))
            
            metrics = SystemMetrics(
                timestamp=get_current_timestamp(),
                cpu_percent=cpu_percent,
                memory_percent=memory.percent,
                memory_used_mb=memory.used / (1024 * 1024),
                disk_usage_percent=(disk_usage.used / disk_usage.total) * 100,
                disk_free_gb=disk_usage.free / (1024 * 1024 * 1024)
            )
            
            with self._lock:
                self.system_metrics.append(metrics)
            
            return metrics
            
        except Exception as e:
            logger.warning(f"Erro ao coletar métricas do sistema: {e}")
            return SystemMetrics(
                timestamp=get_current_timestamp(),
                cpu_percent=0,
                memory_percent=0,
                memory_used_mb=0,
                disk_usage_percent=0,
                disk_free_gb=0
            )
    
    def _update_daily_stats(self, sync_metrics: SyncMetrics) -> None:
        """
        Atualiza estatísticas diárias.
        
        Args:
            sync_metrics: Métricas da sincronização
        """
        # Usa data como chave (YYYY-MM-DD)
        date_key = time.strftime('%Y-%m-%d', time.localtime(sync_metrics.start_time))
        
        stats = self.daily_stats[date_key]
        stats['syncs_count'] += 1
        stats['total_records'] += sync_metrics.records_extracted
        stats['total_files'] += sync_metrics.files_created
        stats['total_size'] += sync_metrics.total_file_size
        
        if sync_metrics.success:
            stats['success_count'] += 1
        else:
            stats['error_count'] += 1
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """
        Obtém métricas atuais.
        
        Returns:
            Métricas atuais do sistema e sincronização
        """
        with self._lock:
            result = {
                'system': None,
                'current_sync': None,
                'recent_syncs': []
            }
            
            # Métricas do sistema
            if self.system_metrics:
                result['system'] = self.system_metrics[-1].to_dict()
            
            # Sincronização atual
            if self.current_sync:
                current_dict = self.current_sync.to_dict()
                if self.current_sync.start_time:
                    current_dict['elapsed_time'] = get_current_timestamp() - self.current_sync.start_time
                result['current_sync'] = current_dict
            
            # Sincronizações recentes
            result['recent_syncs'] = [s.to_dict() for s in self.sync_metrics[-10:]]
            
            return result
    
    def get_performance_summary(self, hours: int = 24) -> Dict[str, Any]:
        """
        Obtém resumo de performance.
        
        Args:
            hours: Número de horas para análise
            
        Returns:
            Resumo de performance
        """
        cutoff_time = get_current_timestamp() - (hours * 3600)
        
        with self._lock:
            # Filtra sincronizações recentes
            recent_syncs = [
                s for s in self.sync_metrics 
                if s.start_time >= cutoff_time
            ]
            
            if not recent_syncs:
                return {
                    'period_hours': hours,
                    'total_syncs': 0,
                    'success_rate': 0,
                    'avg_duration': 0,
                    'total_records': 0,
                    'total_files': 0,
                    'total_size_mb': 0
                }
            
            # Calcula estatísticas
            successful_syncs = [s for s in recent_syncs if s.success]
            
            total_records = sum(s.records_extracted for s in recent_syncs)
            total_files = sum(s.files_created for s in recent_syncs)
            total_size = sum(s.total_file_size for s in recent_syncs)
            
            durations = [s.duration for s in recent_syncs if s.duration]
            avg_duration = statistics.mean(durations) if durations else 0
            
            return {
                'period_hours': hours,
                'total_syncs': len(recent_syncs),
                'successful_syncs': len(successful_syncs),
                'success_rate': (len(successful_syncs) / len(recent_syncs)) * 100,
                'avg_duration': avg_duration,
                'avg_duration_formatted': format_duration(int(avg_duration)),
                'total_records': total_records,
                'total_files': total_files,
                'total_size_mb': total_size / (1024 * 1024),
                'avg_records_per_sync': total_records / len(recent_syncs),
                'avg_files_per_sync': total_files / len(recent_syncs)
            }
    
    def get_error_analysis(self, hours: int = 24) -> Dict[str, Any]:
        """
        Analisa erros recentes.
        
        Args:
            hours: Número de horas para análise
            
        Returns:
            Análise de erros
        """
        cutoff_time = get_current_timestamp() - (hours * 3600)
        
        with self._lock:
            # Filtra sincronizações com erro
            error_syncs = [
                s for s in self.sync_metrics 
                if s.start_time >= cutoff_time and not s.success
            ]
            
            if not error_syncs:
                return {
                    'period_hours': hours,
                    'total_errors': 0,
                    'error_types': {},
                    'affected_mappings': [],
                    'recent_errors': []
                }
            
            # Agrupa erros por tipo
            error_types = defaultdict(int)
            affected_mappings = set()
            
            for sync in error_syncs:
                if sync.error_message:
                    # Simplifica mensagem de erro para agrupamento
                    error_key = sync.error_message.split(':')[0] if ':' in sync.error_message else sync.error_message
                    error_types[error_key] += 1
                
                affected_mappings.add(sync.mapping_name)
            
            return {
                'period_hours': hours,
                'total_errors': len(error_syncs),
                'error_types': dict(error_types),
                'affected_mappings': list(affected_mappings),
                'recent_errors': [
                    {
                        'mapping_name': s.mapping_name,
                        'error_message': s.error_message,
                        'timestamp': s.start_time,
                        'retry_count': s.retry_count
                    }
                    for s in error_syncs[-10:]  # Últimos 10 erros
                ]
            }


class MetricsStorage:
    """Armazenamento persistente de métricas."""
    
    def __init__(self, storage_dir: Path = None):
        """
        Inicializa o armazenamento.
        
        Args:
            storage_dir: Diretório de armazenamento
        """
        self.paths = BridgePaths()
        self.storage_dir = storage_dir or self.paths.get_logs_dir()
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        self.metrics_file = self.storage_dir / "sync_metrics.jsonl"
        self.daily_stats_file = self.storage_dir / "daily_stats.json"
    
    def save_sync_metrics(self, metrics: SyncMetrics) -> None:
        """
        Salva métricas de sincronização.
        
        Args:
            metrics: Métricas a serem salvas
        """
        try:
            with open(self.metrics_file, 'a', encoding='utf-8') as f:
                json.dump(metrics.to_dict(), f, ensure_ascii=False)
                f.write('\n')
            
            logger.debug(f"Métricas salvas: {metrics.mapping_name}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar métricas: {e}")
    
    def save_daily_stats(self, daily_stats: Dict[str, Dict[str, Any]]) -> None:
        """
        Salva estatísticas diárias.
        
        Args:
            daily_stats: Estatísticas diárias
        """
        try:
            with open(self.daily_stats_file, 'w', encoding='utf-8') as f:
                json.dump(daily_stats, f, ensure_ascii=False, indent=2)
            
            logger.debug("Estatísticas diárias salvas")
            
        except Exception as e:
            logger.error(f"Erro ao salvar estatísticas diárias: {e}")
    
    def load_sync_metrics(self, days: int = 30) -> List[SyncMetrics]:
        """
        Carrega métricas de sincronização.
        
        Args:
            days: Número de dias para carregar
            
        Returns:
            Lista de métricas
        """
        metrics = []
        cutoff_time = get_current_timestamp() - (days * 24 * 3600)
        
        try:
            if not self.metrics_file.exists():
                return metrics
            
            with open(self.metrics_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        
                        # Filtra por data
                        if data.get('start_time', 0) >= cutoff_time:
                            # Converte de volta para SyncMetrics
                            sync_metrics = SyncMetrics(**data)
                            metrics.append(sync_metrics)
                            
                    except json.JSONDecodeError:
                        continue
            
            logger.debug(f"Carregadas {len(metrics)} métricas dos últimos {days} dias")
            
        except Exception as e:
            logger.error(f"Erro ao carregar métricas: {e}")
        
        return metrics
    
    def load_daily_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Carrega estatísticas diárias.
        
        Returns:
            Estatísticas diárias
        """
        try:
            if not self.daily_stats_file.exists():
                return {}
            
            with open(self.daily_stats_file, 'r', encoding='utf-8') as f:
                return json.load(f)
                
        except Exception as e:
            logger.error(f"Erro ao carregar estatísticas diárias: {e}")
            return {}
    
    def cleanup_old_metrics(self, days: int = 90) -> None:
        """
        Remove métricas antigas.
        
        Args:
            days: Número de dias para manter
        """
        try:
            if not self.metrics_file.exists():
                return
            
            cutoff_time = get_current_timestamp() - (days * 24 * 3600)
            temp_file = self.metrics_file.with_suffix('.tmp')
            
            kept_count = 0
            removed_count = 0
            
            with open(self.metrics_file, 'r', encoding='utf-8') as input_f, \
                 open(temp_file, 'w', encoding='utf-8') as output_f:
                
                for line in input_f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        
                        if data.get('start_time', 0) >= cutoff_time:
                            output_f.write(line + '\n')
                            kept_count += 1
                        else:
                            removed_count += 1
                            
                    except json.JSONDecodeError:
                        # Mantém linhas inválidas
                        output_f.write(line + '\n')
            
            # Substitui arquivo original
            temp_file.replace(self.metrics_file)
            
            logger.info(f"Limpeza de métricas: {kept_count} mantidas, {removed_count} removidas")
            
        except Exception as e:
            logger.error(f"Erro na limpeza de métricas: {e}")


# Instância global do coletor
_metrics_collector = None
_metrics_storage = None


def get_metrics_collector() -> MetricsCollector:
    """
    Obtém instância global do coletor de métricas.
    
    Returns:
        Instância do coletor
    """
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


def get_metrics_storage() -> MetricsStorage:
    """
    Obtém instância global do armazenamento de métricas.
    
    Returns:
        Instância do armazenamento
    """
    global _metrics_storage
    if _metrics_storage is None:
        _metrics_storage = MetricsStorage()
    return _metrics_storage


def save_metrics_on_exit(collector: MetricsCollector, storage: MetricsStorage) -> None:
    """
    Salva métricas ao sair da aplicação.
    
    Args:
        collector: Coletor de métricas
        storage: Armazenamento de métricas
    """
    try:
        # Salva métricas pendentes
        for sync_metrics in collector.sync_metrics:
            storage.save_sync_metrics(sync_metrics)
        
        # Salva estatísticas diárias
        storage.save_daily_stats(dict(collector.daily_stats))
        
        logger.info("Métricas salvas ao sair da aplicação")
        
    except Exception as e:
        logger.error(f"Erro ao salvar métricas na saída: {e}")


def format_metrics_report(metrics: Dict[str, Any]) -> str:
    """
    Formata relatório de métricas para exibição.
    
    Args:
        metrics: Dados de métricas
        
    Returns:
        Relatório formatado
    """
    lines = []
    
    # Cabeçalho
    lines.append("=== RELATÓRIO DE MÉTRICAS ===")
    lines.append("")
    
    # Sistema atual
    if metrics.get('system'):
        sys = metrics['system']
        lines.append("Sistema:")
        lines.append(f"  CPU: {sys['cpu_percent']:.1f}%")
        lines.append(f"  Memória: {sys['memory_percent']:.1f}% ({sys['memory_used_mb']:.0f} MB)")
        lines.append(f"  Disco: {sys['disk_usage_percent']:.1f}% ({sys['disk_free_gb']:.1f} GB livres)")
        lines.append("")
    
    # Sincronização atual
    if metrics.get('current_sync'):
        sync = metrics['current_sync']
        lines.append("Sincronização Atual:")
        lines.append(f"  Mapeamento: {sync['mapping_name']}")
        lines.append(f"  Schema: {sync['schema_slug']}")
        if sync.get('elapsed_time'):
            lines.append(f"  Tempo decorrido: {format_duration(sync['elapsed_time'])}")
        lines.append(f"  Registros extraídos: {sync['records_extracted']}")
        lines.append("")
    
    # Sincronizações recentes
    if metrics.get('recent_syncs'):
        lines.append("Sincronizações Recentes:")
        for sync in metrics['recent_syncs'][-5:]:  # Últimas 5
            status = "✓" if sync['success'] else "✗"
            duration = format_duration(sync['duration']) if sync['duration'] else "N/A"
            lines.append(f"  {status} {sync['mapping_name']} - {sync['records_extracted']} registros ({duration})")
        lines.append("")
    
    return "\n".join(lines)