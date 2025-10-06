"""
Módulo para escrita de arquivos JSONL (JSON Lines).
Gerencia a criação e escrita de arquivos no formato JSONL para upload.
"""

import json
import gzip
import logging
from typing import Dict, Any, List, Optional, Iterator, Union
from pathlib import Path
from dataclasses import dataclass
import time
import hashlib
from contextlib import contextmanager

from core.paths import BridgePaths
from core.timeutil import get_current_timestamp, format_duration


logger = logging.getLogger(__name__)


@dataclass
class JSONLFileInfo:
    """Informações sobre um arquivo JSONL."""
    file_path: Path
    record_count: int
    file_size: int
    compressed: bool
    checksum: str
    created_at: int
    mapping_name: str
    schema_slug: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        return {
            'file_path': str(self.file_path),
            'record_count': self.record_count,
            'file_size': self.file_size,
            'compressed': self.compressed,
            'checksum': self.checksum,
            'created_at': self.created_at,
            'mapping_name': self.mapping_name,
            'schema_slug': self.schema_slug
        }


class JSONLWriter:
    """Escritor de arquivos JSONL."""
    
    def __init__(self, mapping_name: str, schema_slug: str, 
                 output_dir: Path = None, compress: bool = True):
        """
        Inicializa o escritor JSONL.
        
        Args:
            mapping_name: Nome do mapeamento
            schema_slug: Slug do schema
            output_dir: Diretório de saída (padrão: .bridge/tmp/uploads)
            compress: Se deve comprimir o arquivo
        """
        self.mapping_name = mapping_name
        self.schema_slug = schema_slug
        self.compress = compress
        
        # Configura diretórios
        self.paths = BridgePaths()
        self.output_dir = output_dir or self.paths.uploads_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Estado do arquivo
        self.file_path: Optional[Path] = None
        self.file_handle = None
        self.record_count = 0
        self.start_time = 0
        self.checksum_hash = hashlib.sha256()
        
        # Gera nome do arquivo
        timestamp = int(time.time())
        filename = f"{mapping_name}_{schema_slug}_{timestamp}"
        
        if compress:
            filename += ".jsonl.gz"
        else:
            filename += ".jsonl"
        
        self.file_path = self.output_dir / filename
    
    def __enter__(self):
        """Context manager entry."""
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def open(self) -> None:
        """Abre o arquivo para escrita."""
        if self.file_handle:
            return
        
        self.start_time = get_current_timestamp()
        
        try:
            if self.compress:
                self.file_handle = gzip.open(self.file_path, 'wt', encoding='utf-8')
            else:
                self.file_handle = open(self.file_path, 'w', encoding='utf-8')
            
            logger.info(f"Arquivo JSONL aberto: {self.file_path}")
            
        except Exception as e:
            logger.error(f"Erro ao abrir arquivo JSONL: {e}")
            raise
    
    def close(self) -> JSONLFileInfo:
        """
        Fecha o arquivo e retorna informações.
        
        Returns:
            Informações do arquivo criado
        """
        if not self.file_handle:
            raise RuntimeError("Arquivo não está aberto")
        
        try:
            self.file_handle.close()
            self.file_handle = None
            
            # Obtém informações do arquivo
            file_size = self.file_path.stat().st_size
            checksum = self.checksum_hash.hexdigest()
            
            end_time = get_current_timestamp()
            duration = end_time - self.start_time
            
            logger.info(
                f"Arquivo JSONL fechado: {self.file_path.name} "
                f"({self.record_count} registros, {file_size} bytes, "
                f"{format_duration(duration)})"
            )
            
            return JSONLFileInfo(
                file_path=self.file_path,
                record_count=self.record_count,
                file_size=file_size,
                compressed=self.compress,
                checksum=checksum,
                created_at=self.start_time,
                mapping_name=self.mapping_name,
                schema_slug=self.schema_slug
            )
            
        except Exception as e:
            logger.error(f"Erro ao fechar arquivo JSONL: {e}")
            raise
    
    def write_record(self, record: Dict[str, Any]) -> None:
        """
        Escreve um registro no arquivo.
        
        Args:
            record: Registro a ser escrito
        """
        if not self.file_handle:
            raise RuntimeError("Arquivo não está aberto")
        
        try:
            # Serializa o registro
            json_line = json.dumps(record, ensure_ascii=False, separators=(',', ':'))
            
            # Escreve no arquivo
            self.file_handle.write(json_line + '\n')
            
            # Atualiza checksum
            self.checksum_hash.update(json_line.encode('utf-8'))
            
            # Incrementa contador
            self.record_count += 1
            
        except Exception as e:
            logger.error(f"Erro ao escrever registro: {e}")
            raise
    
    def write_batch(self, records: List[Dict[str, Any]]) -> None:
        """
        Escreve um lote de registros.
        
        Args:
            records: Lista de registros
        """
        for record in records:
            self.write_record(record)
    
    def write_from_iterator(self, records: Iterator[Dict[str, Any]], 
                           batch_size: int = 1000) -> None:
        """
        Escreve registros de um iterador.
        
        Args:
            records: Iterador de registros
            batch_size: Tamanho do lote para logging
        """
        batch_count = 0
        
        for record in records:
            self.write_record(record)
            
            # Log de progresso
            if self.record_count % batch_size == 0:
                batch_count += 1
                logger.debug(f"Escritos {self.record_count} registros (lote {batch_count})")
    
    def flush(self) -> None:
        """Força a escrita dos dados em buffer."""
        if self.file_handle:
            self.file_handle.flush()
    
    def get_current_size(self) -> int:
        """
        Obtém o tamanho atual do arquivo.
        
        Returns:
            Tamanho em bytes
        """
        if self.file_path and self.file_path.exists():
            return self.file_path.stat().st_size
        return 0
    
    def get_progress_info(self) -> Dict[str, Any]:
        """
        Obtém informações de progresso.
        
        Returns:
            Informações de progresso
        """
        current_time = get_current_timestamp()
        elapsed_time = current_time - self.start_time if self.start_time else 0
        
        return {
            'record_count': self.record_count,
            'file_size': self.get_current_size(),
            'elapsed_time': elapsed_time,
            'records_per_second': self.record_count / elapsed_time if elapsed_time > 0 else 0,
            'file_path': str(self.file_path) if self.file_path else None
        }


class JSONLBatchWriter:
    """Escritor JSONL otimizado para grandes volumes de dados."""
    
    def __init__(self, mapping_name: str, schema_slug: str,
                 output_dir: Path = None, compress: bool = True,
                 max_file_size: int = 100 * 1024 * 1024,  # 100MB
                 max_records_per_file: int = 1000000):
        """
        Inicializa o escritor em lotes.
        
        Args:
            mapping_name: Nome do mapeamento
            schema_slug: Slug do schema
            output_dir: Diretório de saída
            compress: Se deve comprimir os arquivos
            max_file_size: Tamanho máximo por arquivo em bytes
            max_records_per_file: Número máximo de registros por arquivo
        """
        self.mapping_name = mapping_name
        self.schema_slug = schema_slug
        self.compress = compress
        self.max_file_size = max_file_size
        self.max_records_per_file = max_records_per_file
        
        # Configura diretórios
        self.paths = BridgePaths()
        self.output_dir = output_dir or self.paths.uploads_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Estado
        self.current_writer: Optional[JSONLWriter] = None
        self.file_sequence = 1
        self.total_records = 0
        self.created_files: List[JSONLFileInfo] = []
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def _create_new_writer(self) -> JSONLWriter:
        """Cria um novo escritor JSONL."""
        # Nome com sequência
        mapping_name = f"{self.mapping_name}_part{self.file_sequence:03d}"
        
        writer = JSONLWriter(
            mapping_name=mapping_name,
            schema_slug=self.schema_slug,
            output_dir=self.output_dir,
            compress=self.compress
        )
        
        writer.open()
        self.file_sequence += 1
        
        return writer
    
    def _should_rotate_file(self) -> bool:
        """Verifica se deve rotacionar o arquivo."""
        if not self.current_writer:
            return True
        
        # Verifica tamanho do arquivo
        if self.current_writer.get_current_size() >= self.max_file_size:
            return True
        
        # Verifica número de registros
        if self.current_writer.record_count >= self.max_records_per_file:
            return True
        
        return False
    
    def write_record(self, record: Dict[str, Any]) -> None:
        """
        Escreve um registro, rotacionando arquivos se necessário.
        
        Args:
            record: Registro a ser escrito
        """
        # Rotaciona arquivo se necessário
        if self._should_rotate_file():
            if self.current_writer:
                file_info = self.current_writer.close()
                self.created_files.append(file_info)
            
            self.current_writer = self._create_new_writer()
        
        # Escreve o registro
        self.current_writer.write_record(record)
        self.total_records += 1
    
    def write_batch(self, records: List[Dict[str, Any]]) -> None:
        """
        Escreve um lote de registros.
        
        Args:
            records: Lista de registros
        """
        for record in records:
            self.write_record(record)
    
    def close(self) -> List[JSONLFileInfo]:
        """
        Fecha todos os arquivos e retorna informações.
        
        Returns:
            Lista de informações dos arquivos criados
        """
        if self.current_writer:
            file_info = self.current_writer.close()
            self.created_files.append(file_info)
            self.current_writer = None
        
        logger.info(
            f"Escritor em lotes finalizado: {len(self.created_files)} arquivos, "
            f"{self.total_records} registros totais"
        )
        
        return self.created_files
    
    def get_progress_info(self) -> Dict[str, Any]:
        """
        Obtém informações de progresso.
        
        Returns:
            Informações de progresso
        """
        current_file_info = {}
        if self.current_writer:
            current_file_info = self.current_writer.get_progress_info()
        
        return {
            'total_records': self.total_records,
            'files_created': len(self.created_files),
            'current_file': current_file_info,
            'current_file_sequence': self.file_sequence - 1
        }


@contextmanager
def create_jsonl_writer(mapping_name: str, schema_slug: str,
                       output_dir: Path = None, compress: bool = True,
                       batch_mode: bool = False, **kwargs):
    """
    Context manager para criar escritores JSONL.
    
    Args:
        mapping_name: Nome do mapeamento
        schema_slug: Slug do schema
        output_dir: Diretório de saída
        compress: Se deve comprimir
        batch_mode: Se deve usar modo em lotes
        **kwargs: Argumentos adicionais para o escritor em lotes
        
    Yields:
        Instância do escritor
    """
    if batch_mode:
        writer = JSONLBatchWriter(
            mapping_name=mapping_name,
            schema_slug=schema_slug,
            output_dir=output_dir,
            compress=compress,
            **kwargs
        )
    else:
        writer = JSONLWriter(
            mapping_name=mapping_name,
            schema_slug=schema_slug,
            output_dir=output_dir,
            compress=compress
        )
    
    try:
        with writer as w:
            yield w
    finally:
        pass


def validate_jsonl_file(file_path: Path) -> Dict[str, Any]:
    """
    Valida um arquivo JSONL.
    
    Args:
        file_path: Caminho do arquivo
        
    Returns:
        Resultado da validação
    """
    result = {
        'valid': False,
        'record_count': 0,
        'errors': [],
        'file_size': 0
    }
    
    try:
        if not file_path.exists():
            result['errors'].append("Arquivo não encontrado")
            return result
        
        result['file_size'] = file_path.stat().st_size
        
        # Abre o arquivo (com suporte a compressão)
        if file_path.suffix == '.gz':
            file_handle = gzip.open(file_path, 'rt', encoding='utf-8')
        else:
            file_handle = open(file_path, 'r', encoding='utf-8')
        
        with file_handle:
            line_number = 0
            for line in file_handle:
                line_number += 1
                line = line.strip()
                
                if not line:
                    continue
                
                try:
                    json.loads(line)
                    result['record_count'] += 1
                except json.JSONDecodeError as e:
                    result['errors'].append(f"Linha {line_number}: {e}")
                    if len(result['errors']) >= 10:  # Limita erros
                        result['errors'].append("... (mais erros omitidos)")
                        break
        
        result['valid'] = len(result['errors']) == 0
        
    except Exception as e:
        result['errors'].append(f"Erro ao validar arquivo: {e}")
    
    return result