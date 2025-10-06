"""
Módulo para upload de arquivos JSONL para a API DataSnap.
Gerencia o processo de upload com retry, progresso e validação.
"""

import logging
import time
import hashlib
from typing import Dict, Any, List, Optional, Callable
from pathlib import Path
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datasnap.api import DataSnapAPI
from sync.token_cache import TokenCache
from sync.jsonl_writer import JSONLFileInfo
from core.timeutil import get_current_timestamp, format_duration
from core.paths import BridgePaths


logger = logging.getLogger(__name__)


@dataclass
class UploadResult:
    """Resultado de um upload."""
    success: bool
    file_info: JSONLFileInfo
    upload_id: Optional[str] = None
    error_message: Optional[str] = None
    upload_time: Optional[int] = None
    upload_duration: Optional[int] = None
    bytes_uploaded: Optional[int] = None
    retry_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        return {
            'success': self.success,
            'file_path': str(self.file_info.file_path),
            'upload_id': self.upload_id,
            'error_message': self.error_message,
            'upload_time': self.upload_time,
            'upload_duration': self.upload_duration,
            'bytes_uploaded': self.bytes_uploaded,
            'retry_count': self.retry_count,
            'record_count': self.file_info.record_count,
            'file_size': self.file_info.file_size
        }


@dataclass
class UploadProgress:
    """Progresso de upload."""
    bytes_uploaded: int
    total_bytes: int
    percentage: float
    speed_bps: float
    eta_seconds: Optional[int] = None
    
    def __str__(self) -> str:
        """Representação em string."""
        speed_mb = self.speed_bps / (1024 * 1024)
        eta_str = f"{self.eta_seconds}s" if self.eta_seconds else "N/A"
        return f"{self.percentage:.1f}% ({speed_mb:.1f} MB/s, ETA: {eta_str})"


class UploadProgressTracker:
    """Rastreador de progresso de upload."""
    
    def __init__(self, total_bytes: int, callback: Optional[Callable[[UploadProgress], None]] = None):
        """
        Inicializa o rastreador.
        
        Args:
            total_bytes: Total de bytes a serem enviados
            callback: Função de callback para atualizações
        """
        self.total_bytes = total_bytes
        self.callback = callback
        self.bytes_uploaded = 0
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.last_bytes = 0
    
    def update(self, bytes_read: int) -> None:
        """
        Atualiza o progresso.
        
        Args:
            bytes_read: Bytes lidos nesta iteração
        """
        self.bytes_uploaded += bytes_read
        current_time = time.time()
        
        # Calcula velocidade
        time_diff = current_time - self.last_update_time
        if time_diff >= 1.0:  # Atualiza a cada segundo
            bytes_diff = self.bytes_uploaded - self.last_bytes
            speed_bps = bytes_diff / time_diff if time_diff > 0 else 0
            
            # Calcula ETA
            remaining_bytes = self.total_bytes - self.bytes_uploaded
            eta_seconds = int(remaining_bytes / speed_bps) if speed_bps > 0 else None
            
            # Calcula porcentagem
            percentage = (self.bytes_uploaded / self.total_bytes) * 100 if self.total_bytes > 0 else 0
            
            progress = UploadProgress(
                bytes_uploaded=self.bytes_uploaded,
                total_bytes=self.total_bytes,
                percentage=percentage,
                speed_bps=speed_bps,
                eta_seconds=eta_seconds
            )
            
            if self.callback:
                self.callback(progress)
            
            self.last_update_time = current_time
            self.last_bytes = self.bytes_uploaded


class FileUploader:
    """Uploader de arquivos individuais."""
    
    def __init__(self, api: DataSnapAPI, token_cache: TokenCache,
                 max_retries: int = 3, chunk_size: int = 8192,
                 timeout: int = 300):
        """
        Inicializa o uploader.
        
        Args:
            api: Instância da API DataSnap
            token_cache: Cache de tokens
            max_retries: Número máximo de tentativas
            chunk_size: Tamanho do chunk para upload
            timeout: Timeout em segundos
        """
        self.api = api
        self.token_cache = token_cache
        self.max_retries = max_retries
        self.chunk_size = chunk_size
        self.timeout = timeout
        
        # Configura sessão HTTP com retry
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def upload_file(self, file_info: JSONLFileInfo, schema_slug: str,
                   progress_callback: Optional[Callable[[UploadProgress], None]] = None) -> UploadResult:
        """
        Faz upload de um arquivo.
        
        Args:
            file_info: Informações do arquivo
            schema_slug: Slug do schema
            progress_callback: Callback de progresso
            
        Returns:
            Resultado do upload
        """
        start_time = get_current_timestamp()
        retry_count = 0
        
        for attempt in range(self.max_retries + 1):
            try:
                logger.info(f"Iniciando upload: {file_info.file_path.name} (tentativa {attempt + 1})")
                
                # Obtém token de upload
                token_info = self._get_upload_token(schema_slug)
                if not token_info:
                    return UploadResult(
                        success=False,
                        file_info=file_info,
                        error_message="Falha ao obter token de upload",
                        retry_count=attempt
                    )
                
                # Faz o upload
                upload_id = self._perform_upload(
                    file_info, token_info, progress_callback
                )
                
                if upload_id:
                    # Notifica conclusão
                    success = self._notify_upload_completion(upload_id, file_info)
                    
                    if success:
                        end_time = get_current_timestamp()
                        return UploadResult(
                            success=True,
                            file_info=file_info,
                            upload_id=upload_id,
                            upload_time=start_time,
                            upload_duration=end_time - start_time,
                            bytes_uploaded=file_info.file_size,
                            retry_count=attempt
                        )
                
            except Exception as e:
                retry_count = attempt
                logger.warning(f"Tentativa {attempt + 1} falhou: {e}")
                
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt  # Backoff exponencial
                    logger.info(f"Aguardando {wait_time}s antes da próxima tentativa...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Upload falhou após {self.max_retries + 1} tentativas")
        
        return UploadResult(
            success=False,
            file_info=file_info,
            error_message=f"Upload falhou após {self.max_retries + 1} tentativas",
            retry_count=retry_count
        )
    
    def _get_upload_token(self, schema_slug: str) -> Optional[Dict[str, Any]]:
        """
        Obtém token de upload (com cache).
        
        Args:
            schema_slug: Slug do schema
            
        Returns:
            Informações do token ou None
        """
        try:
            # Verifica cache primeiro
            cached_token = self.token_cache.get_token(schema_slug)
            if cached_token:
                logger.debug(f"Usando token em cache para {schema_slug}")
                return cached_token
            
            # Obtém novo token
            logger.debug(f"Obtendo novo token para {schema_slug}")
            token_info = self.api.get_upload_token(schema_slug)
            
            if token_info:
                # Armazena no cache
                self.token_cache.store_token(schema_slug, token_info)
                return token_info
            
        except Exception as e:
            logger.error(f"Erro ao obter token de upload: {e}")
        
        return None
    
    def _perform_upload(self, file_info: JSONLFileInfo, token_info: Dict[str, Any],
                       progress_callback: Optional[Callable[[UploadProgress], None]] = None) -> Optional[str]:
        """
        Executa o upload do arquivo.
        
        Args:
            file_info: Informações do arquivo
            token_info: Informações do token
            progress_callback: Callback de progresso
            
        Returns:
            ID do upload ou None
        """
        try:
            upload_url = token_info['upload_url']
            upload_id = token_info['upload_id']
            
            # Configura rastreador de progresso
            tracker = None
            if progress_callback:
                tracker = UploadProgressTracker(file_info.file_size, progress_callback)
            
            # Prepara dados do formulário
            files = {
                'file': (
                    file_info.file_path.name,
                    self._file_generator(file_info.file_path, tracker),
                    'application/octet-stream'
                )
            }
            
            data = {
                'upload_id': upload_id,
                'checksum': file_info.checksum
            }
            
            # Faz o upload
            response = self.session.post(
                upload_url,
                files=files,
                data=data,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            
            logger.info(f"Upload concluído: {file_info.file_path.name}")
            return upload_id
            
        except Exception as e:
            logger.error(f"Erro durante upload: {e}")
            return None
    
    def _file_generator(self, file_path: Path, tracker: Optional[UploadProgressTracker] = None):
        """
        Gerador para leitura do arquivo com progresso.
        
        Args:
            file_path: Caminho do arquivo
            tracker: Rastreador de progresso
            
        Yields:
            Chunks do arquivo
        """
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                
                if tracker:
                    tracker.update(len(chunk))
                
                yield chunk
    
    def _notify_upload_completion(self, upload_id: str, file_info: JSONLFileInfo) -> bool:
        """
        Notifica a API sobre a conclusão do upload.
        
        Args:
            upload_id: ID do upload
            file_info: Informações do arquivo
            
        Returns:
            True se bem-sucedido
        """
        try:
            result = self.api.notify_upload_completion(
                upload_id=upload_id,
                file_size=file_info.file_size,
                record_count=file_info.record_count,
                checksum=file_info.checksum
            )
            
            return result.get('success', False)
            
        except Exception as e:
            logger.error(f"Erro ao notificar conclusão do upload: {e}")
            return False


class BatchUploader:
    """Uploader para múltiplos arquivos."""
    
    def __init__(self, api: DataSnapAPI, token_cache: TokenCache,
                 max_concurrent: int = 3, **uploader_kwargs):
        """
        Inicializa o uploader em lote.
        
        Args:
            api: Instância da API DataSnap
            token_cache: Cache de tokens
            max_concurrent: Número máximo de uploads simultâneos
            **uploader_kwargs: Argumentos para FileUploader
        """
        self.api = api
        self.token_cache = token_cache
        self.max_concurrent = max_concurrent
        self.uploader_kwargs = uploader_kwargs
    
    def upload_files(self, files_info: List[JSONLFileInfo], schema_slug: str,
                    progress_callback: Optional[Callable[[str, UploadProgress], None]] = None) -> List[UploadResult]:
        """
        Faz upload de múltiplos arquivos.
        
        Args:
            files_info: Lista de informações dos arquivos
            schema_slug: Slug do schema
            progress_callback: Callback de progresso (recebe nome do arquivo e progresso)
            
        Returns:
            Lista de resultados
        """
        results = []
        
        if not files_info:
            return results
        
        logger.info(f"Iniciando upload em lote: {len(files_info)} arquivos")
        
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            # Submete tarefas
            future_to_file = {}
            
            for file_info in files_info:
                uploader = FileUploader(self.api, self.token_cache, **self.uploader_kwargs)
                
                # Callback específico do arquivo
                file_progress_callback = None
                if progress_callback:
                    file_progress_callback = lambda p, fname=file_info.file_path.name: progress_callback(fname, p)
                
                future = executor.submit(
                    uploader.upload_file,
                    file_info,
                    schema_slug,
                    file_progress_callback
                )
                
                future_to_file[future] = file_info
            
            # Coleta resultados
            for future in as_completed(future_to_file):
                file_info = future_to_file[future]
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result.success:
                        logger.info(f"Upload bem-sucedido: {file_info.file_path.name}")
                    else:
                        logger.error(f"Upload falhou: {file_info.file_path.name} - {result.error_message}")
                        
                except Exception as e:
                    logger.error(f"Erro no upload de {file_info.file_path.name}: {e}")
                    results.append(UploadResult(
                        success=False,
                        file_info=file_info,
                        error_message=str(e)
                    ))
        
        # Estatísticas finais
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        
        logger.info(f"Upload em lote concluído: {successful} sucessos, {failed} falhas")
        
        return results
    
    def get_upload_summary(self, results: List[UploadResult]) -> Dict[str, Any]:
        """
        Gera resumo dos uploads.
        
        Args:
            results: Lista de resultados
            
        Returns:
            Resumo dos uploads
        """
        if not results:
            return {
                'total_files': 0,
                'successful': 0,
                'failed': 0,
                'total_bytes': 0,
                'total_records': 0,
                'total_duration': 0,
                'average_speed_mbps': 0
            }
        
        successful_results = [r for r in results if r.success]
        failed_results = [r for r in results if not r.success]
        
        total_bytes = sum(r.file_info.file_size for r in results)
        total_records = sum(r.file_info.record_count for r in results)
        
        # Calcula duração total e velocidade média
        total_duration = 0
        total_uploaded_bytes = 0
        
        for result in successful_results:
            if result.upload_duration and result.bytes_uploaded:
                total_duration += result.upload_duration
                total_uploaded_bytes += result.bytes_uploaded
        
        average_speed_mbps = 0
        if total_duration > 0:
            average_speed_bps = total_uploaded_bytes / total_duration
            average_speed_mbps = average_speed_bps / (1024 * 1024)
        
        return {
            'total_files': len(results),
            'successful': len(successful_results),
            'failed': len(failed_results),
            'total_bytes': total_bytes,
            'total_records': total_records,
            'total_duration': total_duration,
            'average_speed_mbps': average_speed_mbps,
            'success_rate': len(successful_results) / len(results) * 100 if results else 0,
            'failed_files': [r.file_info.file_path.name for r in failed_results]
        }


def cleanup_uploaded_files(results: List[UploadResult], keep_failed: bool = True) -> None:
    """
    Remove arquivos que foram enviados com sucesso.
    
    Args:
        results: Lista de resultados de upload
        keep_failed: Se deve manter arquivos que falharam no upload
    """
    removed_count = 0
    
    for result in results:
        should_remove = result.success or not keep_failed
        
        if should_remove and result.file_info.file_path.exists():
            try:
                result.file_info.file_path.unlink()
                removed_count += 1
                logger.debug(f"Arquivo removido: {result.file_info.file_path.name}")
            except Exception as e:
                logger.warning(f"Erro ao remover arquivo {result.file_info.file_path.name}: {e}")
    
    logger.info(f"Limpeza concluída: {removed_count} arquivos removidos")


def validate_upload_requirements(files_info: List[JSONLFileInfo]) -> Dict[str, Any]:
    """
    Valida se os arquivos estão prontos para upload.
    
    Args:
        files_info: Lista de informações dos arquivos
        
    Returns:
        Resultado da validação
    """
    validation = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'total_size': 0,
        'total_records': 0
    }
    
    for file_info in files_info:
        # Verifica se arquivo existe
        if not file_info.file_path.exists():
            validation['errors'].append(f"Arquivo não encontrado: {file_info.file_path.name}")
            validation['valid'] = False
            continue
        
        # Verifica tamanho
        actual_size = file_info.file_path.stat().st_size
        if actual_size != file_info.file_size:
            validation['warnings'].append(
                f"Tamanho divergente em {file_info.file_path.name}: "
                f"esperado {file_info.file_size}, atual {actual_size}"
            )
        
        # Verifica se não está vazio
        if file_info.record_count == 0:
            validation['warnings'].append(f"Arquivo vazio: {file_info.file_path.name}")
        
        validation['total_size'] += actual_size
        validation['total_records'] += file_info.record_count
    
    # Verifica limites
    max_file_size = 500 * 1024 * 1024  # 500MB por arquivo
    max_total_size = 2 * 1024 * 1024 * 1024  # 2GB total
    
    for file_info in files_info:
        if file_info.file_size > max_file_size:
            validation['errors'].append(
                f"Arquivo muito grande: {file_info.file_path.name} "
                f"({file_info.file_size / (1024*1024):.1f}MB > 500MB)"
            )
            validation['valid'] = False
    
    if validation['total_size'] > max_total_size:
        validation['errors'].append(
            f"Total muito grande: {validation['total_size'] / (1024*1024*1024):.1f}GB > 2GB"
        )
        validation['valid'] = False
    
    return validation