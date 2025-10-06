"""Módulo para gerenciar mapeamentos entre tabelas e schemas"""

import os
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

from core.logger import logger
from core.paths import get_bridge_config_dir
from core.datasources_store import DataSourcesStore
from core.secrets_store import secrets_store
from core.http import http_client
from core.database_connector import create_database_connector
from setup.ui_helpers import show_header, show_error_message, show_success_message, show_info_message, show_paginated_table


def run_mappings_menu() -> None:
    """Executa o menu de mapeamentos tabela → schema"""
    show_header()
    
    # 1. Selecionar fonte de dados
    datasource = _select_datasource()
    if not datasource:
        return
    
    # 2. Selecionar tabela
    table = _select_table(datasource)
    if not table:
        return
    
    # 3. Selecionar API Key
    api_key_name, api_key = _select_api_key()
    if not api_key:
        return
    
    # 4. Selecionar schema
    schema = _select_schema(api_key)
    if not schema:
        return
    
    # 5. Configurar mapeamento
    mapping_config = _configure_mapping(datasource, table, api_key_name, schema)
    if not mapping_config:
        return
    
    # 6. Salvar mapeamento
    if _save_mapping(mapping_config):
        show_success_message(f"✅ Vínculo salvo em .bridge/mappings/{datasource['name']}.{table}.json")
        
        # 7. Opção de gerar modelos JSONL
        choice = input("\n[G] Gerar modelos JSONL agora   [0] Voltar ao menu\n> ").strip().upper()
        if choice == 'G':
            _generate_jsonl_models(datasource, table, mapping_config)


def _select_datasource() -> Optional[Dict[str, Any]]:
    """Seleciona uma fonte de dados"""
    try:
        store = DataSourcesStore()
        datasources = store.list_datasources()
        
        if not datasources:
            show_error_message("Nenhuma fonte de dados encontrada. Configure uma fonte primeiro.")
            return None
        
        print("\nSelecione a fonte:")
        for i, ds in enumerate(datasources, 1):
            # Converter DataSource para dict para compatibilidade
            ds_dict = {
                'id': ds.id,
                'name': ds.name,
                'type': ds.type,
                'created_at': ds.created_at,
                'connection': {
                    'type': ds.type,
                    'host': ds.conn.host,
                    'port': ds.conn.port,
                    'database': ds.conn.database,
                    'user': ds.conn.user,
                    'password': ds.conn.password,
                    'options': ds.conn.options
                },
                'tables': {
                    'selected': ds.tables.selected if ds.tables else [],
                    'last_discovery_at': ds.tables.last_discovery_at if ds.tables else None
                }
            }
            
            conn = ds_dict['connection']
            print(f"[{i}] {ds_dict['name']} ({conn['type']}) {conn['host']}:{conn['port']}/{conn['database']}")
        print("[0] Voltar")
        
        choice = input("> ").strip()
        if choice == "0":
            return None
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(datasources):
                # Retornar o dict convertido
                ds = datasources[idx]
                return {
                    'id': ds.id,
                    'name': ds.name,
                    'type': ds.type,
                    'created_at': ds.created_at,
                    'connection': {
                        'type': ds.type,
                        'host': ds.conn.host,
                        'port': ds.conn.port,
                        'database': ds.conn.database,
                        'user': ds.conn.user,
                        'password': ds.conn.password,
                        'options': ds.conn.options
                    },
                    'tables': {
                        'selected': ds.tables.selected if ds.tables else [],
                        'last_discovery_at': ds.tables.last_discovery_at if ds.tables else None
                    }
                }
        except ValueError:
            pass
        
        show_error_message("Opção inválida")
        return None
        
    except Exception as e:
        logger.error(f"Erro ao listar fontes de dados: {e}")
        show_error_message("Erro ao carregar fontes de dados")
        return None


def _select_table(datasource: Dict[str, Any]) -> Optional[str]:
    """Seleciona uma tabela da fonte de dados"""
    try:
        # Obter tabelas selecionadas da fonte de dados
        tables_info = datasource.get('tables', {})
        selected_tables = tables_info.get('selected', [])
        
        if not selected_tables:
            show_error_message(f"Nenhuma tabela selecionada em '{datasource['name']}'. Configure as tabelas primeiro.")
            choice = input("Deseja abrir o fluxo de seleção de tabelas agora? [s/N]: ").strip().lower()
            if choice == 's':
                # TODO: Implementar fluxo de seleção de tabelas
                show_info_message("Fluxo de seleção de tabelas ainda não implementado")
            return None
        
        print(f"\nTabelas selecionadas em '{datasource['name']}':")
        print("#  Tabela")
        for i, table in enumerate(selected_tables, 1):
            print(f"{i}  {table}")
        print("[0] Voltar")
        
        choice = input("> ").strip()
        if choice == "0":
            return None
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(selected_tables):
                return selected_tables[idx]
        except ValueError:
            pass
        
        show_error_message("Opção inválida")
        return None
        
    except Exception as e:
        logger.error(f"Erro ao listar tabelas: {e}")
        show_error_message("Erro ao carregar tabelas")
        return None


def _select_api_key() -> tuple[Optional[str], Optional[str]]:
    """Seleciona uma API Key"""
    try:
        api_keys = secrets_store.list_keys()
        
        if not api_keys:
            show_error_message("Nenhuma API Key encontrada. Configure uma API Key primeiro.")
            return None, None
        
        print("\nSelecione a API Key:")
        for i, key in enumerate(api_keys, 1):
            print(f"[{i}] {key.name}")
        print("[0] Voltar")
        
        choice = input("> ").strip()
        if choice == "0":
            return None, None
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(api_keys):
                key = api_keys[idx]
                return key.name, key.token
        except ValueError:
            pass
        
        show_error_message("Opção inválida")
        return None, None
        
    except Exception as e:
        logger.error(f"Erro ao listar API Keys: {e}")
        show_error_message("Erro ao carregar API Keys")
        return None, None


def _select_schema(api_key: str) -> Optional[Dict[str, Any]]:
    """Seleciona um schema usando a API Key"""
    try:
        print("\nCarregando schemas...")
        
        # Usar o cliente HTTP para buscar schemas
        success, schemas_data = http_client.get_schemas(api_key)
        
        if not success:
            show_error_message(f"Erro ao carregar schemas: {schemas_data}")
            return None
        
        # Verificar se a resposta tem o formato esperado baseado no JSON fornecido
        if 'data' not in schemas_data:
            show_error_message("Formato de resposta inválido - 'data' não encontrado")
            return None
        
        schemas = schemas_data['data']
        
        if not schemas:
            show_error_message("Nenhum schema encontrado")
            return None
        
        # Configurar paginação
        items_per_page = 10
        current_page = 1
        
        while True:
            print("\n" + "="*60)
            print("ESCOLHER SCHEMA DE DESTINO")
            print("="*60)
            
            # Definir colunas para a tabela
            columns = [
                {"key": "id", "title": "ID"},
                {"key": "slug", "title": "Slug"},
                {"key": "name", "title": "Nome do Schema"}
            ]
            
            # Exibir tabela paginada
            pagination_info = show_paginated_table(
                items=schemas,
                columns=columns,
                title="Schemas Disponíveis",
                items_per_page=items_per_page,
                current_page=current_page
            )
            
            # Opções de navegação
            print("\nOpções:")
            print("[1-{}] Selecionar schema pelo número".format(min(items_per_page, len(schemas))))
            
            if pagination_info["has_prev"]:
                print("[p] Página anterior")
            if pagination_info["has_next"]:
                print("[n] Próxima página")
            
            print("[0] Voltar")
            
            choice = input("\n> ").strip().lower()
            
            if choice == "0":
                return None
            elif choice == "p" and pagination_info["has_prev"]:
                current_page -= 1
                continue
            elif choice == "n" and pagination_info["has_next"]:
                current_page += 1
                continue
            else:
                # Tentar converter para número
                try:
                    idx = int(choice) - 1
                    
                    # Calcular índice real baseado na página atual
                    start_idx = (current_page - 1) * items_per_page
                    real_idx = start_idx + idx
                    
                    if 0 <= idx < min(items_per_page, len(schemas) - start_idx) and real_idx < len(schemas):
                        selected_schema = schemas[real_idx]
                        show_success_message(f"Schema selecionado: {selected_schema.get('name', 'Sem nome')} (ID: {selected_schema.get('id', 'N/A')})")
                        return selected_schema
                    else:
                        show_error_message("Número inválido. Escolha um número da lista.")
                except ValueError:
                    show_error_message("Opção inválida. Use um número, 'p', 'n' ou '0'.")
        
    except Exception as e:
        logger.error(f"Erro ao buscar schemas: {e}")
        show_error_message("Erro ao conectar com a API. Verifique a API Key e conexão.")
        return None


def _configure_mapping(datasource: Dict[str, Any], table: str, api_key_name: str, schema: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Configura as opções do mapeamento"""
    print(f"\nConfiguração do vínculo para {datasource['name']}.{table} → schema \"{schema.get('name', 'N/A')} ({schema.get('id', 'N/A')})\"")
    
    # Detectar chave primária automaticamente
    detected_pk = None
    try:
        db_connector = create_database_connector(datasource)
        detected_pk = db_connector.get_primary_key_column(table)
        if detected_pk:
            show_info_message(f"Chave primária detectada: {detected_pk}")
    except Exception as e:
        logger.warning(f"Não foi possível detectar chave primária: {e}")
    
    # delete_after_upload
    delete_after = input("Excluir dados após upload? [s/N]: ").strip().lower() == 's'
    
    # incremental_mode
    print("incremental_mode:")
    print("1. full")
    print("2. incremental_pk")
    print("3. incremental_timestamp")
    print("4. custom_sql")
    mode_choice = input("Escolha [1-4]: ").strip()
    
    mode_map = {
        '1': 'full',
        '2': 'incremental_pk',
        '3': 'incremental_timestamp',
        '4': 'custom_sql'
    }
    incremental_mode = mode_map.get(mode_choice, 'full')
    
    # pk_column (se necessário)
    pk_column = None
    if incremental_mode == 'incremental_pk' or delete_after:
        default_pk = f" [{detected_pk}]" if detected_pk else ""
        pk_column = input(f"pk_column (ex.: id){default_pk}: ").strip()
        if not pk_column and detected_pk:
            pk_column = detected_pk
        if not pk_column:
            show_error_message("pk_column é obrigatório para este modo")
            return None
    
    # initial_watermark
    initial_watermark = input("initial_watermark [0]: ").strip() or "0"
    
    # batch_size
    batch_size_input = input("batch_size [5000]: ").strip()
    try:
        batch_size = int(batch_size_input) if batch_size_input else 5000
    except ValueError:
        batch_size = 5000
    
    # order_by
    default_order = f"{pk_column} ASC" if pk_column else "id ASC"
    order_by = input(f"order_by [{default_order}]: ").strip() or default_order
    
    # Confirmar exclusão se delete_after_upload
    delete_safety_enabled = False
    if delete_after and pk_column:
        confirm_delete = input(f"Confirmar exclusão pós-upload usando {pk_column}? (DELETE seguro) [s/N]: ").strip().lower() == 's'
        delete_safety_enabled = confirm_delete
    
    # Montar configuração
    mapping_config = {
        "version": 1,
        "source": {
            "name": datasource['name'],
            "type": datasource['connection']['type'],
            "connection_ref": datasource['name']
        },
        "table": table,
        "schema": {
            "id": schema.get('id'),
            "name": schema.get('name'),
            "token_ref": api_key_name
        },
        "transfer": {
            "incremental_mode": incremental_mode,
            "pk_column": pk_column,
            "timestamp_column": None,
            "initial_watermark": initial_watermark,
            "batch_size": batch_size,
            "order_by": order_by,
            "delete_after_upload": delete_after,
            "delete_safety": {
                "enabled": delete_safety_enabled,
                "where_column": pk_column if delete_safety_enabled else None
            }
        },
        "notes": "Arquivo gerado via Bridge Setup. Editável."
    }
    
    return mapping_config


def _save_mapping(mapping_config: Dict[str, Any]) -> bool:
    """Salva o mapeamento em arquivo JSON"""
    try:
        _ensure_mapping_directories()
        
        source_name = mapping_config['source']['name']
        table_name = mapping_config['table']
        
        # Caminho do arquivo de mapeamento
        mappings_dir = os.path.join(get_bridge_config_dir(), "mappings")
        mapping_file = os.path.join(mappings_dir, f"{source_name}.{table_name}.json")
        
        # Salvar arquivo
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping_config, f, indent=2, ensure_ascii=False)
        
        # Criar arquivo de estado inicial
        _create_initial_state_file(source_name, table_name)
        
        logger.info(f"Mapeamento salvo: {mapping_file}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar mapeamento: {e}")
        show_error_message("Erro ao salvar mapeamento")
        return False


def _create_initial_state_file(source_name: str, table_name: str) -> None:
    """Cria arquivo de estado inicial"""
    try:
        state_dir = os.path.join(get_bridge_config_dir(), "mappings_state")
        state_file = os.path.join(state_dir, f"{source_name}.{table_name}.state.json")
        
        # Não sobrescrever se já existir
        if os.path.exists(state_file):
            return
        
        initial_state = {
            "last_synced": {
                "watermark": "0",
                "at": None
            },
            "counters": {
                "runs": 0,
                "uploaded_rows_total": 0,
                "deleted_rows_total": 0
            },
            "last_run": {
                "started_at": None,
                "finished_at": None,
                "uploaded_rows": 0,
                "deleted_rows": 0,
                "status": "never_run"
            }
        }
        
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(initial_state, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Estado inicial criado: {state_file}")
        
    except Exception as e:
        logger.error(f"Erro ao criar estado inicial: {e}")


def _ensure_mapping_directories() -> None:
    """Garante que os diretórios de mapeamento existam"""
    bridge_dir = get_bridge_config_dir()
    
    directories = [
        os.path.join(bridge_dir, "mappings"),
        os.path.join(bridge_dir, "mappings_state"),
        os.path.join(bridge_dir, "models")
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)


def _generate_jsonl_models(datasource: Dict[str, Any], table: str, mapping_config: Dict[str, Any]) -> None:
    """Gera arquivos de modelo JSONL com dados reais da tabela"""
    try:
        print(f"\nGerando modelo JSONL para {datasource['name']}.{table}...")
        
        # Criar diretório de modelos
        models_dir = os.path.join(get_bridge_config_dir(), "models", datasource['name'])
        os.makedirs(models_dir, exist_ok=True)
        
        # Caminho do arquivo JSONL
        jsonl_file = os.path.join(models_dir, f"{table}.jsonl")
        
        # Conectar ao banco e obter dados
        db_connector = create_database_connector(datasource)
        
        # Obter estrutura das colunas
        columns = db_connector.get_table_columns(table)
        if not columns:
            show_error_message("Não foi possível obter estrutura da tabela")
            return
        
        # Obter dados de amostra
        pk_column = mapping_config['transfer'].get('pk_column')
        sample_data = db_connector.sample_table_data(table, limit=100, order_by=pk_column)
        
        with open(jsonl_file, 'w', encoding='utf-8') as f:
            if sample_data:
                # Escrever dados reais
                for record in sample_data:
                    # Converter valores para tipos JSON serializáveis
                    json_record = {}
                    for key, value in record.items():
                        if isinstance(value, datetime):
                            json_record[key] = value.isoformat()
                        elif value is None:
                            json_record[key] = None
                        else:
                            json_record[key] = value
                    
                    f.write(json.dumps(json_record, ensure_ascii=False, default=str) + '\n')
                
                # Preencher até 100 linhas com registros vazios se necessário
                for i in range(len(sample_data), 100):
                    empty_record = {}
                    for column in columns:
                        empty_record[column['name']] = ""
                    f.write(json.dumps(empty_record, ensure_ascii=False) + '\n')
                
                show_success_message(f"✅ Modelo JSONL gerado com {len(sample_data)} registros reais: {jsonl_file}")
            else:
                # Tabela vazia - criar 100 linhas em branco
                for i in range(100):
                    empty_record = {}
                    for column in columns:
                        empty_record[column['name']] = ""
                    f.write(json.dumps(empty_record, ensure_ascii=False) + '\n')
                
                show_success_message(f"✅ Modelo JSONL gerado com 100 linhas em branco (tabela vazia): {jsonl_file}")
        
    except Exception as e:
        logger.error(f"Erro ao gerar modelo JSONL: {e}")
        show_error_message(f"Erro ao gerar modelo JSONL: {e}")