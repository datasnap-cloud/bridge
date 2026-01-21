"""
Módulo principal para execução de sincronizações.
Gerencia o processo completo de extração, transformação e upload de dados.
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
from core.secrets_store import secrets_store
from core.timeutil import Timer, get_current_timestamp, format_duration
from datasnap.api import DataSnapAPI
from sync.extractor import extract_mapping_data, test_source_connection
from sync.jsonl_writer import JSONLBatchWriter, JSONLFileInfo
from sync.metrics import get_metrics_collector
from sync.token_cache import TokenCache
from sync.uploader import BatchUploader, UploadProgress, cleanup_uploaded_files
from core.telemetry import telemetry
from core.http import http_client


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
        self._run_ids: Dict[str, int] = {}
        self.logger.info(f"[DEBUG] SyncRunner.__init__ concluído")

    def _send_telemetry(self, event_type: str, status: str, mapping_config: Optional[Dict] = None, **kwargs):
        """Helper para envio seguro de telemetria"""
        try:
            # Obter token válido
            # Obter token válido
            token = None
            try:
                # Tenta obter API Key do sistema (preferencial)
                keys = secrets_store.list_keys()
                if keys:
                    token = keys[0].token
            except Exception as e:
                self.logger.warning(f"⚠️ Não foi possível obter token para telemetria: {e}")

            # Determinar source e destination
            source = "datasnap-bridge"
            destination = "datasnap-cloud"
            
            if mapping_config:
                source = mapping_config.get('source', {}).get('name') or mapping_config.get('source_type') or "unknown"
                destination = mapping_config.get('schema_slug') or mapping_config.get('schema', {}).get('slug') or "unknown"
            
            # Construir payload
            payload = telemetry.build_payload(
                event_type=event_type,
                status=status,
                source=source,
                destination=destination,
                **kwargs
            )
            
            # Enviar (sem bloquear ou falhar o processo principal)
            # Como send_healthcheck é síncrono (requests), isso adiciona latência.
            # Idealmente seria async, mas Runner é async def, http_client é sync.
            # Para evitar travar loop, poderíamos usar thread, mas vamos manter simples por enquanto.
            # Enviar e capturar resposta
            success, response = http_client.send_healthcheck(secret="", payload=payload, token=token)
            if not success:
               self.logger.warning(f"⚠️ Falha no envio de telemetria ({event_type}): {response}")
               return None
            
            return response

        except Exception as e:
            self.logger.warning(f"⚠️ Erro ao processar telemetria: {e}")
        
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
                available = self._get_available_mappings()
                msg = f"Configuração do mapeamento '{mapping_name}' não encontrada."
                if available:
                    mapping_list = ", ".join(sorted(available))
                    msg += f"\n\nMapeamentos disponíveis:\n{mapping_list}"
                else:
                    msg += " Nenhum mapeamento disponível encontrado."
                raise ValueError(msg)
            
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
            
            # Telemetria: Run Start
            start_resp = self._send_telemetry(
                event_type="run_start",
                status="success", # Start é sempre success se chegou aqui
                mapping_config=mapping_config
            )
            
            if start_resp and isinstance(start_resp, dict) and 'id' in start_resp:
                self._run_ids[mapping_name] = start_resp['id']

            
            # Extrair dados
            self.logger.info(f"[DEBUG] Iniciando extração de dados...")
            records = await self._extract_data(mapping_config)
            self.logger.info(f"[DEBUG] Extração concluída. Registros encontrados: {len(records) if records else 0}")
            
            if not records:
                self.logger.warning(f"Nenhum registro encontrado para o mapeamento: {mapping_name}")
                
                # Telemetria: Run End (Empty)
                run_id_val = self._run_ids.get(mapping_name)
                self._send_telemetry(
                    event_type="run_end",
                    status="success",
                    mapping_config=mapping_config,
                    duration_ms=int(timer.elapsed() * 1000),
                    items_processed=0,
                    bytes_uploaded=0,
                    retry_count=0,
                    error_message="Nenhum registro encontrado",
                    id=run_id_val
                )
                
                return SyncResult(
                    mapping_name=mapping_name,
                    success=True,
                    duration_seconds=timer.elapsed()
                )
            
            # Verificar número mínimo de registros para upload
            min_records_for_upload = mapping_config.get('transfer', {}).get('min_records_for_upload', 0)
            records_count = len(records)
            
            if min_records_for_upload > 0 and records_count < min_records_for_upload:
                self.logger.info(f"📊 Registros encontrados: {records_count}")
                self.logger.info(f"📋 Mínimo necessário: {min_records_for_upload}")
                msg = f"Upload cancelado: {records_count} registros encontrados, mínimo necessário: {min_records_for_upload}"
                self.logger.warning(f"⚠️  {msg}")

                # Telemetria: Run End (Skipped)
                run_id_val = self._run_ids.get(mapping_name)
                self._send_telemetry(
                    event_type="run_end",
                    status="success", # Considerado sucesso (skipped)
                    mapping_config=mapping_config,
                    duration_ms=int(timer.elapsed() * 1000),
                    items_processed=records_count,
                    bytes_uploaded=0,
                    retry_count=0,
                    error_message=msg,
                    id=run_id_val
                )

                return SyncResult(
                    mapping_name=mapping_name,
                    success=True,
                    duration_seconds=timer.elapsed(),
                    error_message=msg
                )
            
            self.logger.info(f"✅ Validação de número mínimo passou: {records_count} registros (mínimo: {min_records_for_upload})")
            
            # Escrever arquivos JSONL
            self.logger.info(f"📝 Escrevendo arquivos JSONL...")
            jsonl_files = await self._write_jsonl_files(mapping_name, records)
            self.logger.info(f"✅ Arquivos JSONL criados: {len(jsonl_files)}")
            
            # Upload dos arquivos
            upload_success = True
            upload_error = None
            upload_error_details = None
            files_uploaded = 0
            if not self.config.dry_run:
                self.logger.info(f"🔄 Convertendo Path objects para JSONLFileInfo...")
                # Converter Path objects para JSONLFileInfo
                from sync.jsonl_writer import JSONLFileInfo
                from core.timeutil import get_current_timestamp
                import hashlib
                
                files_info = []
                for file_path in jsonl_files:
                    if file_path.exists():
                        file_size = file_path.stat().st_size
                        # Calcular checksum simples
                        with open(file_path, 'rb') as f:
                            checksum = hashlib.md5(f.read()).hexdigest()
                        
                        file_info = JSONLFileInfo(
                            file_path=file_path,
                            record_count=0,  # Será atualizado pelo writer
                            file_size=file_size,
                            compressed=file_path.suffix == '.gz',
                            checksum=checksum,
                            created_at=get_current_timestamp(),
                            mapping_name=mapping_name,
                            schema_slug=schema_slug
                        )
                        files_info.append(file_info)
                        self.logger.debug(f"📄 Arquivo convertido: {file_path.name} -> {file_size} bytes, checksum: {checksum[:8]}...")
                
                self.logger.info(f"🚀 Iniciando upload de {len(files_info)} arquivos...")
                upload_success, upload_error, upload_error_details = self._upload_files(files_info, mapping_name)
                files_uploaded = len(files_info) if upload_success else 0
                self.logger.info(f"📊 Upload concluído: sucesso={upload_success}, arquivos enviados={files_uploaded}")
            else:
                self.logger.info(f"🔄 Modo dry-run ativo - pulando upload")
            
            # Calcular estatísticas
            total_records = len(records)
            files_created = len(jsonl_files)
            
            # Deletar registros do banco se delete_after_upload estiver habilitado e upload foi bem-sucedido
            if upload_success and total_records > 0:
                await self._handle_delete_after_upload(mapping_config, records, mapping_name)
            
            # Atualizar watermark se houver registros e modo incremental
            if total_records > 0:
                self._update_watermark(mapping_config, records)
            
            # Determinar se a sincronização foi bem-sucedida
            sync_success = upload_success or self.config.dry_run  # Dry-run sempre é considerado sucesso
            
            # Atualizar estado baseado no resultado do upload
            if sync_success:
                self.state_store.finish_sync_success(
                    mapping_name, 
                    total_records
                )
            else:
                self.state_store.finish_sync_error(mapping_name, "Falha no upload de arquivos")
            
            self.metrics.finish_sync_metrics(success=sync_success, 
                                           error_message=None if sync_success else "Falha no upload de arquivos")
            
            result = SyncResult(
                mapping_name=mapping_name,
                success=sync_success,
                records_processed=total_records,
                files_created=files_created,
                files_uploaded=files_uploaded,
                duration_seconds=timer.elapsed(),
                error_message=None if sync_success else "Falha no upload de arquivos"
            )
            
            self.logger.info(
                f"Sincronização concluída: {mapping_name} "
                f"({total_records} registros, {files_uploaded} arquivos, "
                f"{format_duration(timer.elapsed())})"
            )
            
            # Telemetria: Run End
            run_id_val = self._run_ids.get(mapping_name)
            
            # Prepara kwargs de erro se houver
            telemetry_error_kwargs = {}
            if not sync_success and upload_error_details:
                telemetry_error_kwargs = upload_error_details
                
            self._send_telemetry(
                event_type="run_end",
                status="success" if sync_success else "error",
                mapping_config=mapping_config,
                duration_ms=int(timer.elapsed() * 1000),
                items_processed=total_records,
                bytes_uploaded=total_size if 'total_size' in locals() else 0,
                retry_count=total_retries if 'total_retries' in locals() else 0,
                error_message=None if sync_success else (upload_error or "Falha no upload"),
                id=run_id_val,
                **telemetry_error_kwargs
            )

            return result
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Erro na sincronização do mapeamento {mapping_name}: {error_msg}")
            
            # Limpar arquivos temporários em caso de erro
            try:
                self.logger.info(f"🧹 Limpando arquivos temporários após erro...")
                self._cleanup_temp_files_for_mapping(mapping_name)
            except Exception as cleanup_error:
                self.logger.warning(f"⚠️ Erro durante limpeza de arquivos temporários: {cleanup_error}")
            
            self.state_store.finish_sync_error(mapping_name, error_msg)
            self.metrics.finish_sync_metrics(success=False, error_message=error_msg)
            
            # Telemetria: Error
            # Tentar carregar mapping_config se não existir (para contexto)
            current_config = locals().get('mapping_config')
            
            self._send_telemetry(
                event_type="error",
                status="error",
                mapping_config=current_config,
                duration_ms=int(timer.elapsed() * 1000),
                error_message=error_msg,
                error_stack=str(e), # stacktrace seria melhor, mas msg serve por enquanto
                id=self._run_ids.get(mapping_name)
            )

            return SyncResult(
                mapping_name=mapping_name,
                success=False,
                error_message=error_msg,
                duration_seconds=timer.elapsed()
            )
        finally:
            self._run_ids.pop(mapping_name, None)
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
                name: state.last_sync_timestamp 
                for name, state in states.items()
                if state.last_sync_timestamp
            },
            'sync_counts': {
                name: state.sync_count 
                for name, state in states.items()
            },
            'error_counts': {
                name: 1 if state.last_error else 0
                for name, state in states.items()
                if state.last_error
            }
        }
    
    def _load_mapping_config(self, mapping_name: str) -> Optional[Dict]:
        """Carrega a configuração de um mapeamento."""
        try:
            self.logger.debug(f"📋 Iniciando carregamento da configuração do mapeamento: {mapping_name}")
            mapping_file = self.paths.get_mapping_file(mapping_name)
            self.logger.debug(f"📁 Caminho do arquivo de mapeamento: {mapping_file}")
            
            if not mapping_file.exists():
                self.logger.error(f"❌ Arquivo de mapeamento não encontrado: {mapping_file}")
                return None
            
            file_size = mapping_file.stat().st_size
            self.logger.debug(f"📊 Tamanho do arquivo: {file_size} bytes")
            
            import json
            with open(mapping_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            self.logger.info(f"✅ Configuração do mapeamento carregada com sucesso de: {mapping_file}")
            self.logger.info(f"📜 Conteúdo da configuração: {json.dumps(config, indent=2, default=str)}")
            self.logger.debug(f"🔧 Fonte de dados: {config.get('source_type', 'N/A')}")
            self.logger.debug(f"🏷️ Schema slug: {config.get('schema_slug', 'N/A')}")
            self.logger.debug(f"🗂️ Tabela: {config.get('table_name', 'N/A')}")
            
            return config
        except Exception as e:
            self.logger.error(f"💥 Erro ao carregar mapeamento {mapping_name}: {e}")
            return None
    
    async def _test_source_connection(self, mapping_config: Dict) -> None:
        """Testa a conexão com a fonte de dados."""
        try:
            self.logger.info(f"🔌 Testando conexão com a fonte de dados...")
            self.logger.debug(f"🔧 Tipo da fonte: {mapping_config.get('source_type', 'N/A')}")
            self.logger.debug(f"🏠 Host: {mapping_config.get('host', 'N/A')}")
            self.logger.debug(f"🗂️ Database: {mapping_config.get('database', 'N/A')}")
            
            success, error = await asyncio.to_thread(
                test_source_connection, 
                mapping_config
            )
            
            if not success:
                self.logger.error(f"❌ Falha na conexão com a fonte: {error}")
                raise ConnectionError(f"Falha na conexão com a fonte: {error}")
            
            self.logger.info(f"✅ Conexão com a fonte de dados estabelecida com sucesso")
            
        except Exception as e:
            self.logger.error(f"💥 Erro ao testar conexão: {e}")
            raise ConnectionError(f"Erro ao testar conexão: {e}")
    
    async def _extract_data(self, mapping_config: Dict) -> List[Dict]:
        """Extrai dados da fonte."""
        try:
            self.logger.info(f"📊 Iniciando extração de dados...")
            self.logger.debug(f"🔧 Configuração: {mapping_config.get('source_type', 'N/A')} -> {mapping_config.get('table_name', 'N/A')}")
            
            extraction_result = await asyncio.to_thread(
                extract_mapping_data, 
                mapping_config
            )
            
            # extract_mapping_data retorna ExtractionResult, não uma lista
            if not extraction_result.success:
                self.logger.error(f"❌ Falha na extração: {extraction_result.error_message}")
                raise RuntimeError(f"Falha na extração: {extraction_result.error_message}")
            
            records_count = len(extraction_result.data) if extraction_result.data else 0
            self.logger.info(f"✅ Extração concluída com sucesso: {records_count} registros extraídos")
            
            if records_count > 0:
                self.logger.debug(f"📋 Primeiro registro (amostra): {extraction_result.data[0] if extraction_result.data else 'N/A'}")
            
            # Agora retornamos os dados reais do ExtractionResult
            return extraction_result.data or []
            
        except Exception as e:
            self.logger.error(f"💥 Erro na extração de dados: {e}")
            raise RuntimeError(f"Erro na extração de dados: {e}")
    
    async def _write_jsonl_files(
        self, 
        mapping_name: str, 
        records: List[Dict]
    ) -> List[Path]:
        """Escreve os registros em arquivos JSONL."""
        try:
            self.logger.info(f"📝 Iniciando escrita de arquivos JSONL...")
            self.logger.debug(f"📊 Total de registros para escrever: {len(records)}")
            
            output_dir = self.paths.uploads_dir
            self.logger.debug(f"📁 Diretório de saída: {output_dir}")
            
            batch_writer = JSONLBatchWriter(
                mapping_name=mapping_name,
                schema_slug=mapping_name,  # Usando mapping_name como schema_slug por enquanto
                output_dir=output_dir,
                compress=False,  # Alterado para False para gerar arquivos .jsonl em vez de .gz
                max_records_per_file=self.config.batch_size,
                max_file_size=self.config.max_file_size_mb * 1024 * 1024  # Convertendo MB para bytes
            )
            
            self.logger.debug(f"🔧 Configuração do writer: batch_size={self.config.batch_size}, max_file_size={self.config.max_file_size_mb}MB, compress=False")
            
            with batch_writer:
                batch_writer.write_batch(records)
                files_info = batch_writer.close()
            
            file_paths = [file_info.file_path for file_info in files_info]
            self.logger.info(f"✅ Arquivos JSONL criados com sucesso: {len(file_paths)} arquivo(s)")
            
            # Log detalhado dos arquivos criados
            for i, file_path in enumerate(file_paths):
                if file_path.exists():
                    file_size = file_path.stat().st_size
                    self.logger.debug(f"📄 Arquivo {i+1}: {file_path.name} ({file_size} bytes)")
                else:
                    self.logger.warning(f"⚠️ Arquivo {i+1} não encontrado: {file_path}")
            
            return file_paths
        except Exception as e:
            self.logger.error(f"💥 Erro ao escrever arquivos JSONL: {e}")
            raise RuntimeError(f"Erro ao escrever arquivos JSONL: {e}")
    
    def _upload_files(self, files_info: List[JSONLFileInfo], mapping_name: str) -> bool:
        """
        Faz upload dos arquivos JSONL.
        
        Args:
            files_info: Lista de informações dos arquivos
            mapping_name: Nome do mapeamento
            
        Returns:
            True se todos os uploads foram bem-sucedidos
        """
        if not files_info:
            self.logger.info(f"📤 Nenhum arquivo para upload no mapeamento {mapping_name}")
            return True
        
        start_time = get_current_timestamp()
        self.logger.info(f"📤 Iniciando upload de {len(files_info)} arquivo(s) para o mapeamento {mapping_name}")
        
        # Log detalhado dos arquivos que serão enviados
        total_size = 0
        for i, file_info in enumerate(files_info):
            if file_info.file_path.exists():
                file_size = file_info.file_path.stat().st_size
                total_size += file_size
                self.logger.debug(f"📄 Arquivo {i+1}: {file_info.file_path.name} ({file_size} bytes, {file_info.record_count} registros)")
            else:
                self.logger.warning(f"⚠️ Arquivo {i+1} não encontrado: {file_info.file_path}")
        
        self.logger.info(f"📊 Total de dados para upload: {total_size} bytes ({total_size / 1024 / 1024:.2f} MB)")
        
        try:
            # Carrega configuração do mapping para obter o schema_slug correto
            mapping_config = self._load_mapping_config(mapping_name)
            if not mapping_config:
                self.logger.error(f"❌ Não foi possível carregar configuração do mapping: {mapping_name}")
                return False
                
            schema_slug = mapping_config.get('schema', {}).get('slug')
            if not schema_slug:
                self.logger.error(f"❌ Schema slug não encontrado na configuração do mapping: {mapping_name}")
                return False
                
            self.logger.debug(f"🏷️ Schema slug para {mapping_name}: {schema_slug}")
            
            # Cria uploader
            self.logger.debug(f"🔧 Criando BatchUploader...")
            uploader = BatchUploader(self.api, self.token_cache)
            
            # Callback de progresso
            def progress_callback(filename: str, progress: UploadProgress):
                self.logger.info(f"📈 Upload progress - {filename}: {progress.percentage:.1f}% "
                               f"({progress.bytes_uploaded}/{progress.total_bytes} bytes)")
            
            # Faz upload
            self.logger.info(f"🚀 Iniciando upload para schema {schema_slug}...")
            results = uploader.upload_files(files_info, schema_slug, progress_callback, mapping_name)
            
            # Analisa resultados
            successful_uploads = [r for r in results if r.success]
            failed_uploads = [r for r in results if not r.success]
            
            self.logger.info(f"📊 Resultados do upload: {len(successful_uploads)} sucessos, {len(failed_uploads)} falhas")
            
            # Log detalhado dos resultados
            for result in results:
                if result.success:
                    self.logger.info(f"✅ Upload bem-sucedido: {result.file_info.file_path.name} -> upload_id: {result.upload_id}")
                else:
                    self.logger.error(f"❌ Upload falhou: {result.file_info.file_path.name} -> erro: {result.error_message}")
            
            # Atualiza métricas
            total_records = sum(result.file_info.record_count for result in successful_uploads)
            upload_duration = get_current_timestamp() - start_time
            total_retries = sum(result.retry_count for result in results)
            self.metrics.update_upload_metrics(
                files_uploaded=len(successful_uploads), 
                records_uploaded=total_records,
                duration=upload_duration,
                retry_count=total_retries
            )
            
            if len(failed_uploads) == 0:
                self.logger.info(f"🎉 Todos os uploads foram bem-sucedidos para {mapping_name}")
                error_msg = None
                error_details = None
            else:
                # Coleta mensagens de erro
                error_msgs = [f"{r.file_info.file_path.name}: {r.error_message}" for r in failed_uploads]
                error_msg = "; ".join(error_msgs)
                
                # Coleta detalhes do primeiro erro para telemetria (priorizando o que tem mais informação)
                first_failure = failed_uploads[0]
                error_details = {
                    'error_code': first_failure.error_code,
                    'error_stack': first_failure.error_stack,
                    'error_context': first_failure.error_context
                }
                
                self.logger.warning(f"⚠️ {len(failed_uploads)} upload(s) falharam para {mapping_name}: {error_msg}")
            
            # Limpar arquivos temporários após upload (sucesso ou falha)
            try:
                self.logger.info(f"🧹 Limpando arquivos temporários...")
                cleanup_uploaded_files(results, keep_failed=False)  # Remove todos os arquivos, incluindo os que falharam
            except Exception as e:
                self.logger.warning(f"⚠️ Erro durante limpeza de arquivos temporários: {e}")
            
            return len(failed_uploads) == 0, error_msg, error_details
            
        except Exception as e:
            msg = f"Erro crítico durante processo de upload: {e}"
            self.logger.error(f"💥 {msg}")
            
            import traceback
            error_details = {
                'error_code': type(e).__name__,
                'error_stack': traceback.format_exc(),
                'error_context': {}
            }
            return False, msg, error_details
    
    def _cleanup_temp_files_for_mapping(self, mapping_name: str) -> None:
        """
        Limpa arquivos temporários específicos de um mapeamento.
        
        Args:
            mapping_name: Nome do mapeamento
        """
        try:
            uploads_dir = self.paths.uploads_dir
            if not uploads_dir.exists():
                return
            
            # Procura por arquivos que começam com o nome do mapeamento
            pattern = f"{mapping_name}_*"
            files_to_remove = list(uploads_dir.glob(pattern))
            
            removed_count = 0
            for file_path in files_to_remove:
                try:
                    if file_path.is_file():
                        file_path.unlink()
                        removed_count += 1
                        self.logger.debug(f"Arquivo temporário removido: {file_path.name}")
                except Exception as e:
                    self.logger.warning(f"Erro ao remover arquivo {file_path.name}: {e}")
            
            if removed_count > 0:
                self.logger.info(f"🧹 Limpeza concluída: {removed_count} arquivos temporários removidos para {mapping_name}")
            else:
                self.logger.debug(f"Nenhum arquivo temporário encontrado para {mapping_name}")
                
        except Exception as e:
            self.logger.warning(f"Erro durante limpeza de arquivos temporários para {mapping_name}: {e}")
    
    async def _handle_delete_after_upload(self, mapping_config: Dict, records: List[Dict], mapping_name: str) -> None:
        """
        Deleta registros do banco de dados após upload bem-sucedido, se configurado.
        
        Args:
            mapping_config: Configuração do mapeamento
            records: Lista de registros que foram enviados
            mapping_name: Nome do mapeamento
        """
        try:
            # Verifica se delete_after_upload está habilitado
            transfer_config = mapping_config.get('transfer', {})
            delete_after_upload = transfer_config.get('delete_after_upload', False)
            
            if not delete_after_upload:
                self.logger.debug(f"🔄 delete_after_upload não está habilitado para {mapping_name}, pulando deleção")
                return
            
            if not records:
                self.logger.debug(f"📊 Nenhum registro para deletar em {mapping_name}")
                return
            
            # Identifica a chave primária para deleção
            pk_column = transfer_config.get('pk_column')
            if not pk_column:
                self.logger.error(f"❌ Coluna de chave primária não configurada para delete_after_upload em {mapping_name}")
                return
            
            # Coleta os IDs dos registros para deletar
            record_ids = []
            for record in records:
                if pk_column in record and record[pk_column] is not None:
                    record_ids.append(record[pk_column])
            
            if not record_ids:
                self.logger.warning(f"⚠️ Nenhum ID válido encontrado para deleção em {mapping_name}")
                return
            
            self.logger.info(f"🗑️ Iniciando deleção de {len(record_ids)} registros de {mapping_name}...")
            
            # Executa a deleção
            from sync.extractor import delete_records_after_upload
            
            delete_result = await asyncio.to_thread(
                delete_records_after_upload,
                mapping_config,
                record_ids,
                pk_column
            )
            
            if delete_result.success:
                self.logger.info(f"✅ Deleção concluída com sucesso: {delete_result.deleted_count} registros removidos de {mapping_name}")
                
                # Atualiza métricas de deleção
                self.metrics.update_deletion_metrics(
                    records_deleted=delete_result.deleted_count,
                    mapping_name=mapping_name
                )
            else:
                self.logger.error(f"❌ Falha na deleção de registros em {mapping_name}: {delete_result.error_message}")
                # Não propaga o erro para não falhar a sincronização
            
        except Exception as e:
            self.logger.error(f"💥 Erro durante deleção de registros em {mapping_name}: {e}")
            # Não propaga o erro para não falhar a sincronização

    def _update_watermark(self, mapping_config: Dict, records: List[Dict]) -> None:
        """Atualiza o watermark no arquivo de configuração após sincronização bem-sucedida."""
        try:
            # Verifica se é sincronização incremental
            transfer_config = mapping_config.get('transfer', {})
            incremental_mode = transfer_config.get('incremental_mode', 'full')
            
            if incremental_mode not in ['incremental_pk', 'incremental_timestamp']:
                self.logger.debug(f"🔄 Modo de sincronização não é incremental ({incremental_mode}), pulando atualização de watermark")
                return
            
            # Determina a coluna do watermark baseada no modo
            watermark_column = None
            if incremental_mode == 'incremental_pk':
                watermark_column = transfer_config.get('pk_column')
            elif incremental_mode == 'incremental_timestamp':
                watermark_column = transfer_config.get('timestamp_column')
            
            if not watermark_column:
                self.logger.debug(f"📊 Coluna de watermark não configurada para modo {incremental_mode}, pulando atualização")
                return
            
            if not records:
                self.logger.debug(f"📊 Nenhum registro para atualizar watermark")
                return
            
            # Encontra o maior valor da coluna watermark nos registros
            max_watermark = None
            for record in records:
                if watermark_column in record and record[watermark_column] is not None:
                    current_value = record[watermark_column]
                    if max_watermark is None or current_value > max_watermark:
                        max_watermark = current_value
            
            if max_watermark is None:
                self.logger.warning(f"⚠️ Não foi possível encontrar valor válido para watermark na coluna '{watermark_column}'")
                return
            
            # Atualiza o arquivo de configuração
            mapping_name = f"{mapping_config.get('source', {}).get('name', 'unknown')}.{mapping_config.get('table', 'unknown')}"
            mapping_file = self.paths.get_mapping_file(mapping_name)
            
            if not mapping_file.exists():
                self.logger.error(f"❌ Arquivo de mapeamento não encontrado: {mapping_file}")
                return
            
            # Lê o arquivo atual
            import json
            with open(mapping_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            # Atualiza o watermark
            old_watermark = config_data.get('transfer', {}).get('initial_watermark')
            if 'transfer' not in config_data:
                config_data['transfer'] = {}
            config_data['transfer']['initial_watermark'] = str(max_watermark)
            
            # Salva o arquivo atualizado
            with open(mapping_file, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False, default=str)
            
            self.logger.info(f"🔄 Watermark atualizado para {mapping_name}: {old_watermark} -> {max_watermark}")
            
        except Exception as e:
            self.logger.error(f"💥 Erro ao atualizar watermark: {e}")
            # Não propaga o erro para não falhar a sincronização
    
    def _get_available_mappings(self) -> List[str]:
        """Retorna a lista de mapeamentos disponíveis."""
        try:
            mapping_files = self.paths.list_mapping_files()
            # Converte Path objects para strings (apenas o nome do arquivo sem extensão)
            return [path.stem for path in mapping_files]
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
    
    # Enviar heartbeat único por execução do comando
    try:
        # Tenta obter token silenciosamente
        token = None
        try:
            keys = secrets_store.list_keys()
            if keys:
                token = keys[0].token
        except:
            pass

        hb_payload = telemetry.build_payload(
            event_type="heartbeat",
            status="success",
            source="datasnap-bridge",
            destination="datasnap-cloud"
        )
        
        success, _ = http_client.send_healthcheck(secret="", payload=hb_payload, token=token)
        if not success:
             print(f"❌ Erro ao enviar heartbeat (verifique os logs)")
    except Exception as e:
        print(f"❌ Erro ao enviar heartbeat: {e}")
        logger.warning(f"Falha no heartbeat inicial do comando: {e}")

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