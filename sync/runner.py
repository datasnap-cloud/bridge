"""
Módulo principal para orquestração da sincronização de mapeamentos.

Este módulo coordena todo o pipeline de sincronização, desde a extração
dos dados até o upload para a API DataSnap.
"""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from core.mapping_state_store import MappingStateStore
from core.paths import BridgePaths
from core.timeutil import Timer, get_current_timestamp, format_duration
from datasnap.api import DataSnapAPI
from sync.extractor import extract_mapping_data, test_source_connection
from sync.jsonl_writer import JSONLBatchWriter
from sync.metrics import get_metrics_collector
from sync.token_cache import TokenCache
from sync.uploader import BatchUploader


@dataclass
class SyncConfig:
    """Configuração para execução da sincronização."""
    max_workers: int = 4
    batch_size: int = 10000
    max_file_size_mb: int = 100
    retry_attempts: int = 3
    retry_delay: float = 1.0
    timeout_seconds: int = 3600
    dry_run: bool = False
    force_full_sync: bool = False
    skip_validation: bool = False


@dataclass
class SyncResult:
    """Resultado da sincronização de um mapeamento."""
    mapping_name: str
    success: bool
    records_processed: int = 0
    files_created: int = 0
    files_uploaded: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class SyncRunner:
    """Orquestrador principal da sincronização."""
    
    def __init__(self, config: Optional[SyncConfig] = None):
        self.config = config or SyncConfig()
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"[DEBUG] SyncRunner.__init__ iniciado")
        
        self.paths = BridgePaths()
        self.logger.info(f"[DEBUG] BridgePaths inicializado")
        
        self.state_store = MappingStateStore()
        self.logger.info(f"[DEBUG] MappingStateStore inicializado")
        
        self.api = DataSnapAPI()
        self.logger.info(f"[DEBUG] DataSnapAPI inicializado")
        
        self.token_cache = TokenCache()
        self.logger.info(f"[DEBUG] TokenCache inicializado")
        
        self.metrics = get_metrics_collector()
        self.logger.info(f"[DEBUG] Metrics collector inicializado")
        
        self._running_syncs: Set[str] = set()
        self.logger.info(f"[DEBUG] SyncRunner.__init__ concluído")
        
    async def sync_mapping(self, mapping_name: str) -> SyncResult:
        """
        Sincroniza um mapeamento específico.
        
        Args:
            mapping_name: Nome do mapeamento a ser sincronizado
            
        Returns:
            Resultado da sincronização
        """
        self.logger.info(f"[DEBUG] sync_mapping iniciado para: {mapping_name}")
        
        if mapping_name in self._running_syncs:
            self.logger.info(f"[DEBUG] Sincronização já em execução para {mapping_name}")
            return SyncResult(
                mapping_name=mapping_name,
                success=False,
                error_message="Sincronização já está em execução para este mapeamento"
            )
        
        self.logger.info(f"[DEBUG] Adicionando {mapping_name} aos syncs em execução")
        self._running_syncs.add(mapping_name)
        timer = Timer()
        timer.start()  # Iniciar o timer
        
        try:
            self.logger.info(f"[DEBUG] Iniciando sincronização do mapeamento: {mapping_name}")
            
            self.logger.info(f"[DEBUG] Chamando state_store.start_sync...")
            self.state_store.start_sync(mapping_name)
            self.logger.info(f"[DEBUG] state_store.start_sync concluído")
            
            self.logger.info(f"[DEBUG] Chamando metrics.start_sync_metrics...")
            # Carregar configuração do mapeamento primeiro para obter schema_slug
            self.logger.info(f"[DEBUG] Carregando configuração do mapeamento: {mapping_name}")
            mapping_config = self._load_mapping_config(mapping_name)
            if not mapping_config:
                raise ValueError(f"Configuração do mapeamento '{mapping_name}' não encontrada")
            
            self.logger.info(f"[DEBUG] Configuração carregada com sucesso: {mapping_config}")
            
            # Precisamos do schema_slug para iniciar as métricas
            schema_slug = mapping_config.get('schema_slug', mapping_name)
            self.metrics.start_sync_metrics(mapping_name, schema_slug)
            self.logger.info(f"[DEBUG] metrics.start_sync_metrics concluído")
            
            # Testar conexão com a fonte
            if not self.config.skip_validation:
                self.logger.info(f"[DEBUG] Testando conexão com a fonte de dados...")
                await self._test_source_connection(mapping_config)
                self.logger.info(f"[DEBUG] Teste de conexão concluído")
            else:
                self.logger.info(f"[DEBUG] Pulando validação de conexão (skip_validation=True)")
            
            # Extrair dados
            self.logger.info(f"[DEBUG] Iniciando extração de dados...")
            records = await self._extract_data(mapping_config)
            self.logger.info(f"[DEBUG] Extração concluída. Registros encontrados: {len(records) if records else 0}")
            
            if not records:
                self.logger.warning(f"Nenhum registro encontrado para o mapeamento: {mapping_name}")
                return SyncResult(
                    mapping_name=mapping_name,
                    success=True,
                    duration_seconds=timer.elapsed()
                )
            
            # Escrever arquivos JSONL
            self.logger.info(f"[DEBUG] Escrevendo arquivos JSONL...")
            jsonl_files = await self._write_jsonl_files(mapping_name, records)
            self.logger.info(f"[DEBUG] Arquivos JSONL criados: {len(jsonl_files)}")
            
            # Upload dos arquivos
            upload_results = []
            if not self.config.dry_run:
                upload_results = await self._upload_files(mapping_name, jsonl_files)
            
            # Calcular estatísticas
            total_records = len(records)
            files_created = len(jsonl_files)
            files_uploaded = len([r for r in upload_results if r.success])
            
            # Atualizar estado
            self.state_store.finish_sync_success(
                mapping_name, 
                total_records, 
                files_uploaded
            )
            
            self.metrics.finish_sync_metrics(success=True)
            
            result = SyncResult(
                mapping_name=mapping_name,
                success=True,
                records_processed=total_records,
                files_created=files_created,
                files_uploaded=files_uploaded,
                duration_seconds=timer.elapsed()
            )
            
            self.logger.info(
                f"Sincronização concluída: {mapping_name} "
                f"({total_records} registros, {files_uploaded} arquivos, "
                f"{format_duration(timer.elapsed())})"
            )
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Erro na sincronização do mapeamento {mapping_name}: {error_msg}")
            
            self.state_store.finish_sync_error(mapping_name, error_msg)
            self.metrics.finish_sync_metrics(success=False, error_message=error_msg)
            
            return SyncResult(
                mapping_name=mapping_name,
                success=False,
                error_message=error_msg,
                duration_seconds=timer.elapsed()
            )
        finally:
            self._running_syncs.discard(mapping_name)
    
    async def sync_multiple_mappings(
        self, 
        mapping_names: List[str],
        parallel: bool = True
    ) -> List[SyncResult]:
        """
        Sincroniza múltiplos mapeamentos.
        
        Args:
            mapping_names: Lista de nomes dos mapeamentos
            parallel: Se True, executa em paralelo
            
        Returns:
            Lista de resultados das sincronizações
        """
        self.logger.info(f"[DEBUG] sync_multiple_mappings iniciado com mapeamentos: {mapping_names}, parallel: {parallel}")
        
        if not parallel:
            results = []
            for mapping_name in mapping_names:
                self.logger.info(f"[DEBUG] Processando mapeamento sequencial: {mapping_name}")
                result = await self.sync_mapping(mapping_name)
                results.append(result)
            return results
        
        # Execução paralela
        self.logger.info(f"[DEBUG] Iniciando execução paralela para {len(mapping_names)} mapeamentos")
        tasks = [self.sync_mapping(name) for name in mapping_names]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Converter exceções em resultados de erro
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                final_results.append(SyncResult(
                    mapping_name=mapping_names[i],
                    success=False,
                    error_message=str(result)
                ))
            else:
                final_results.append(result)
        
        self.logger.info(f"[DEBUG] sync_multiple_mappings concluído com {len(final_results)} resultados")
        return final_results
    
    async def sync_all_mappings(self) -> List[SyncResult]:
        """
        Sincroniza todos os mapeamentos disponíveis.
        
        Returns:
            Lista de resultados das sincronizações
        """
        mapping_names = self._get_available_mappings()
        if not mapping_names:
            self.logger.warning("Nenhum mapeamento encontrado")
            return []
        
        self.logger.info(f"Sincronizando {len(mapping_names)} mapeamentos")
        return await self.sync_multiple_mappings(mapping_names, parallel=True)
    
    def get_sync_status(self) -> Dict[str, any]:
        """
        Retorna o status atual das sincronizações.
        
        Returns:
            Dicionário com informações de status
        """
        states = self.state_store.get_all_states()
        
        return {
            'running_syncs': list(self._running_syncs),
            'total_mappings': len(self._get_available_mappings()),
            'last_sync_times': {
                name: state.last_sync_time 
                for name, state in states.items()
                if state.last_sync_time
            },
            'sync_counts': {
                name: state.total_syncs 
                for name, state in states.items()
            },
            'error_counts': {
                name: state.error_count 
                for name, state in states.items()
                if state.error_count > 0
            }
        }
    
    def _load_mapping_config(self, mapping_name: str) -> Optional[Dict]:
        """Carrega a configuração de um mapeamento."""
        try:
            mapping_file = self.paths.get_mapping_file(mapping_name)
            self.logger.info(f"[DEBUG] Tentando carregar arquivo de mapeamento: {mapping_file}")
            if not mapping_file.exists():
                self.logger.error(f"[DEBUG] Arquivo de mapeamento não existe: {mapping_file}")
                return None
            
            import json
            with open(mapping_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.logger.info(f"[DEBUG] Configuração carregada: {config}")
                return config
        except Exception as e:
            self.logger.error(f"Erro ao carregar mapeamento {mapping_name}: {e}")
            return None
    
    async def _test_source_connection(self, mapping_config: Dict) -> None:
        """Testa a conexão com a fonte de dados."""
        try:
            success, error = await asyncio.to_thread(
                test_source_connection, 
                mapping_config
            )
            if not success:
                raise ConnectionError(f"Falha na conexão com a fonte: {error}")
        except Exception as e:
            raise ConnectionError(f"Erro ao testar conexão: {e}")
    
    async def _extract_data(self, mapping_config: Dict) -> List[Dict]:
        """Extrai dados da fonte."""
        try:
            extraction_result = await asyncio.to_thread(
                extract_mapping_data, 
                mapping_config
            )
            
            # extract_mapping_data retorna ExtractionResult, não uma lista
            if not extraction_result.success:
                raise RuntimeError(f"Falha na extração: {extraction_result.error_message}")
            
            # Por enquanto, retornamos uma lista vazia já que não temos os dados reais
            # TODO: Implementar retorno dos dados extraídos do ExtractionResult
            return []
            
        except Exception as e:
            raise RuntimeError(f"Erro na extração de dados: {e}")
    
    async def _write_jsonl_files(
        self, 
        mapping_name: str, 
        records: List[Dict]
    ) -> List[Path]:
        """Escreve os registros em arquivos JSONL."""
        try:
            output_dir = self.paths.get_temp_upload_dir()
            
            batch_writer = JSONLBatchWriter(
                output_dir=output_dir,
                file_prefix=f"{mapping_name}_",
                max_records_per_file=self.config.batch_size,
                max_file_size_mb=self.config.max_file_size_mb,
                compress=True
            )
            
            return await asyncio.to_thread(
                batch_writer.write_records,
                records
            )
        except Exception as e:
            raise RuntimeError(f"Erro ao escrever arquivos JSONL: {e}")
    
    async def _upload_files(
        self, 
        mapping_name: str, 
        jsonl_files: List[Path]
    ) -> List:
        """Faz upload dos arquivos JSONL."""
        try:
            uploader = BatchUploader(
                api=self.api,
                token_cache=self.token_cache,
                max_workers=self.config.max_workers,
                retry_attempts=self.config.retry_attempts
            )
            
            return await uploader.upload_files(
                files=jsonl_files,
                schema_name=mapping_name
            )
        except Exception as e:
            raise RuntimeError(f"Erro no upload dos arquivos: {e}")
    
    def _get_available_mappings(self) -> List[str]:
        """Retorna a lista de mapeamentos disponíveis."""
        try:
            return self.paths.list_mapping_files()
        except Exception as e:
            self.logger.error(f"Erro ao listar mapeamentos: {e}")
            return []


def create_sync_runner(config: Optional[SyncConfig] = None) -> SyncRunner:
    """
    Cria uma instância do SyncRunner.
    
    Args:
        config: Configuração opcional
        
    Returns:
        Instância do SyncRunner
    """
    return SyncRunner(config)


async def run_sync_command(
    mapping_names: Optional[List[str]] = None,
    all_mappings: bool = False,
    parallel: bool = True,
    dry_run: bool = False,
    force: bool = False,
    config: Optional[SyncConfig] = None
) -> List[SyncResult]:
    """
    Executa o comando de sincronização.
    
    Args:
        mapping_names: Lista de mapeamentos específicos
        all_mappings: Se True, sincroniza todos os mapeamentos
        parallel: Se True, executa em paralelo
        dry_run: Se True, não faz upload real
        force: Se True, força sincronização completa
        config: Configuração personalizada
        
    Returns:
        Lista de resultados das sincronizações
    """
    logger = logging.getLogger(__name__)
    logger.info(f"[DEBUG] run_sync_command iniciado com mapping_names: {mapping_names}, all_mappings: {all_mappings}, dry_run: {dry_run}")
    
    try:
        logger.info(f"[DEBUG] Verificando config...")
        if not config:
            logger.info(f"[DEBUG] Criando nova config...")
            config = SyncConfig(
                dry_run=dry_run,
                force_full_sync=force
            )
            logger.info(f"[DEBUG] Config criada: {config}")
        
        logger.info(f"[DEBUG] Prestes a criar SyncRunner...")
        runner = create_sync_runner(config)
        logger.info(f"[DEBUG] SyncRunner criado com sucesso")
        
        if all_mappings:
            logger.info(f"[DEBUG] Executando sync_all_mappings")
            return await runner.sync_all_mappings()
        elif mapping_names:
            logger.info(f"[DEBUG] Executando sync_multiple_mappings com {len(mapping_names)} mapeamentos")
            return await runner.sync_multiple_mappings(mapping_names, parallel)
        else:
            raise ValueError("Especifique mapeamentos ou use --all")
    except Exception as e:
        logger.error(f"[DEBUG] Erro em run_sync_command: {e}")
        raise


def format_sync_results(results: List[SyncResult]) -> str:
    """
    Formata os resultados da sincronização para exibição.
    
    Args:
        results: Lista de resultados
        
    Returns:
        String formatada com os resultados
    """
    if not results:
        return "Nenhuma sincronização executada."
    
    lines = ["Resultados da Sincronização:", "=" * 50]
    
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    
    lines.append(f"Total: {len(results)} | Sucesso: {len(successful)} | Falhas: {len(failed)}")
    lines.append("")
    
    if successful:
        lines.append("✅ Sucessos:")
        for result in successful:
            duration = format_duration(result.duration_seconds)
            lines.append(
                f"  • {result.mapping_name}: {result.records_processed} registros, "
                f"{result.files_uploaded} arquivos, {duration}"
            )
        lines.append("")
    
    if failed:
        lines.append("❌ Falhas:")
        for result in failed:
            lines.append(f"  • {result.mapping_name}: {result.error_message}")
        lines.append("")
    
    total_records = sum(r.records_processed for r in successful)
    total_files = sum(r.files_uploaded for r in successful)
    total_duration = sum(r.duration_seconds for r in results)
    
    lines.append(f"Totais: {total_records} registros, {total_files} arquivos, {format_duration(total_duration)}")
    
    return "\n".join(lines)