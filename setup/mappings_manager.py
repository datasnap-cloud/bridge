"""
Módulo para gerenciar vinculações entre fontes de dados e schemas
"""

import os
import json
import glob
from typing import List, Dict, Optional, Tuple
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.panel import Panel

from core.paths import get_bridge_config_dir
from core.logger import logger
from core.secrets_store import secrets_store
from core.http import http_client
from setup.ui_helpers import wait_for_continue, show_success_message, show_error_message, show_warning_message


console = Console()


class MappingInfo:
    """Classe para representar informações de uma vinculação"""
    
    def __init__(self, file_path: str, data: Dict):
        self.file_path = file_path
        self.filename = os.path.basename(file_path)
        self.source_name = data.get('source', {}).get('name', 'Desconhecido')
        self.source_type = data.get('source', {}).get('type', 'Desconhecido')
        self.table_name = data.get('table', 'Desconhecida')
        self.schema_name = data.get('schema', {}).get('name', 'Desconhecido')
        self.schema_id = data.get('schema', {}).get('id', 'N/A')
        self.schema_slug = data.get('schema', {}).get('slug', None)  # Tentar obter slug do arquivo primeiro
        
    def __str__(self):
        return f"{self.source_name} ({self.source_type}) → {self.table_name} → {self.schema_name}"


def get_all_mappings() -> List[MappingInfo]:
    """
    Obtém todas as vinculações existentes
    
    Returns:
        List[MappingInfo]: Lista de vinculações
    """
    mappings = []
    
    try:
        mappings_dir = os.path.join(get_bridge_config_dir(), "mappings")
        
        if not os.path.exists(mappings_dir):
            return mappings
        
        # Buscar todos os arquivos de mapeamento
        mapping_files = glob.glob(os.path.join(mappings_dir, "*.json"))
        
        for mapping_file in mapping_files:
            try:
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    mapping_data = json.load(f)
                
                mapping_info = MappingInfo(mapping_file, mapping_data)
                mappings.append(mapping_info)
                
            except Exception as e:
                logger.warning(f"Erro ao ler mapeamento {mapping_file}: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Erro ao obter mapeamentos: {e}")
    
    return mappings


def get_schema_slugs(mappings: List[MappingInfo]) -> None:
    """
    Obtém os slugs dos schemas via API para as vinculações
    
    Args:
        mappings: Lista de vinculações para obter slugs
    """
    try:
        # Primeiro, verificar se já temos slugs salvos nos arquivos
        for mapping in mappings:
            if mapping.schema_slug:
                continue  # Já tem slug, pular
        
        # Tentar obter uma API key válida
        api_keys = secrets_store.get_api_keys()
        if not api_keys:
            return
        
        # Usar a primeira API key disponível
        api_key = api_keys[0]
        success, data = http_client.get_schemas(api_key.token)
        
        if success and 'data' in data:
            schemas_by_id = {str(schema.get('id')): schema for schema in data['data']}
            
            for mapping in mappings:
                # Se já tem slug do arquivo, não sobrescrever
                if mapping.schema_slug:
                    continue
                    
                schema_id = str(mapping.schema_id)
                if schema_id in schemas_by_id:
                    mapping.schema_slug = schemas_by_id[schema_id].get('slug', 'N/A')
                else:
                    mapping.schema_slug = 'N/A'
        
    except Exception as e:
        logger.warning(f"Erro ao obter slugs dos schemas: {e}")
        # Para mappings sem slug, definir como N/A
        for mapping in mappings:
            if not mapping.schema_slug:
                mapping.schema_slug = 'N/A'


def list_mappings() -> bool:
    """
    Lista todas as vinculações existentes
    
    Returns:
        bool: True se executou com sucesso
    """
    try:
        console.print("\n[bold cyan]📋 Gerenciar Vinculações[/bold cyan]")
        console.print("─" * 60)
        
        # Obter todas as vinculações
        mappings = get_all_mappings()
        
        if not mappings:
            show_warning_message("Nenhuma vinculação encontrada.")
            return True
        
        # Obter slugs dos schemas
        console.print("[yellow]🔍 Obtendo informações dos schemas...[/yellow]")
        get_schema_slugs(mappings)
        
        # Criar tabela formatada
        table = Table(title="[bold green]Vinculações Ativas[/bold green]")
        table.add_column("#", style="cyan", no_wrap=True, width=3)
        table.add_column("Fonte", style="yellow", min_width=15)
        table.add_column("Tipo", style="magenta", width=8)
        table.add_column("Tabela", style="white", min_width=15)
        table.add_column("Schema", style="green", min_width=20)
        table.add_column("ID", style="cyan", width=5)
        table.add_column("Slug", style="blue", min_width=15)
        table.add_column("Arquivo", style="dim", min_width=20)
        
        # Adicionar vinculações à tabela
        for i, mapping in enumerate(mappings, 1):
            table.add_row(
                str(i),
                mapping.source_name,
                mapping.source_type.upper(),
                mapping.table_name,
                mapping.schema_name,
                str(mapping.schema_id),
                mapping.schema_slug or 'N/A',
                mapping.filename
            )
        
        console.print(table)
        console.print(f"\n[dim]Total: {len(mappings)} vinculação(ões) encontrada(s)[/dim]")
        
        # Menu de opções
        console.print("\n📋 [bold]Opções:[/bold]")
        console.print("  [red]1.[/red] Excluir vinculação")
        console.print("  [cyan]0.[/cyan] Voltar")
        
        choice = Prompt.ask(
            "\n🎯 Escolha uma opção",
            choices=["0", "1"],
            default="0"
        )
        
        if choice == "1":
            return _delete_mapping_interactive(mappings)
        
        return True
        
    except Exception as e:
        logger.exception(f"Erro ao listar vinculações: {e}")
        show_error_message(f"Erro ao listar vinculações: {e}")
        return True


def _delete_mapping_interactive(mappings: List[MappingInfo]) -> bool:
    """
    Interface interativa para excluir vinculações
    
    Args:
        mappings: Lista de vinculações disponíveis
        
    Returns:
        bool: True se executou com sucesso
    """
    try:
        console.print("\n[bold red]🗑️ Excluir Vinculação[/bold red]")
        console.print("─" * 60)
        
        # Mostrar lista numerada
        for i, mapping in enumerate(mappings, 1):
            console.print(f"  [cyan]{i}.[/cyan] {mapping}")
        
        console.print(f"  [cyan]0.[/cyan] Cancelar")
        
        # Obter escolha do usuário
        max_choice = len(mappings)
        valid_choices = [str(i) for i in range(0, max_choice + 1)]
        
        choice = Prompt.ask(
            f"\n🎯 Escolha a vinculação para excluir [0-{max_choice}]",
            choices=valid_choices,
            default="0"
        )
        
        if choice == "0":
            return True
        
        # Obter vinculação selecionada
        selected_mapping = mappings[int(choice) - 1]
        
        # Confirmar exclusão
        console.print(f"\n[yellow]⚠️ Você está prestes a excluir a vinculação:[/yellow]")
        console.print(f"   [white]Fonte:[/white] {selected_mapping.source_name} ({selected_mapping.source_type})")
        console.print(f"   [white]Tabela:[/white] {selected_mapping.table_name}")
        console.print(f"   [white]Schema:[/white] {selected_mapping.schema_name} (ID: {selected_mapping.schema_id})")
        console.print(f"   [white]Arquivo:[/white] {selected_mapping.filename}")
        
        if not Confirm.ask("\n❓ Tem certeza que deseja excluir esta vinculação?", default=False):
            show_warning_message("Operação cancelada.")
            return True
        
        # Excluir vinculação
        return _delete_mapping(selected_mapping)
        
    except Exception as e:
        logger.exception(f"Erro na exclusão interativa: {e}")
        show_error_message(f"Erro na exclusão: {e}")
        return True


def _delete_mapping(mapping: MappingInfo) -> bool:
    """
    Exclui uma vinculação específica
    
    Args:
        mapping: Vinculação a ser excluída
        
    Returns:
        bool: True se executou com sucesso
    """
    try:
        # Excluir arquivo de mapeamento
        if os.path.exists(mapping.file_path):
            os.remove(mapping.file_path)
            logger.info(f"Arquivo de mapeamento excluído: {mapping.file_path}")
        
        # Excluir arquivo de estado correspondente (se existir)
        state_file = mapping.file_path.replace('.json', '.state.json')
        state_dir = os.path.join(get_bridge_config_dir(), "mappings_state")
        state_path = os.path.join(state_dir, os.path.basename(state_file))
        
        if os.path.exists(state_path):
            os.remove(state_path)
            logger.info(f"Arquivo de estado excluído: {state_path}")
        
        show_success_message(f"Vinculação excluída com sucesso: {mapping.source_name} → {mapping.table_name}")
        return True
        
    except Exception as e:
        logger.exception(f"Erro ao excluir vinculação: {e}")
        show_error_message(f"Erro ao excluir vinculação: {e}")
        return True


def run_mappings_manager() -> bool:
    """
    Executa o menu de gerenciamento de vinculações
    
    Returns:
        bool: True se executou com sucesso
    """
    return list_mappings()