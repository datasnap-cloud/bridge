"""
Módulo com ações do menu de setup
"""

import json
import os
import glob
from typing import List, Optional, Tuple
from rich.console import Console
from rich.table import Table
from rich.syntax import Syntax
from rich.panel import Panel
from rich.prompt import Prompt, Confirm

from core.secrets_store import secrets_store, APIKey
from core.http import http_client
from core.logger import logger


console = Console()


def run_api_keys_menu() -> bool:
    """
    Menu de gerenciamento de API Keys
    """
    while True:
        console.clear()
        console.print("\n[bold blue]🔑 Gerenciamento de API Keys[/bold blue]")
        console.print("─" * 60)
        
        # Listar keys atuais
        keys = secrets_store.list_keys()
        if not keys:
            console.print("[dim]Nenhuma API Key cadastrada.[/dim]")
            console.print("\n[bold]Opções:[/bold]")
            console.print("[1] Cadastrar Nova Key")
            console.print("[0] Voltar")
            
            choice = Prompt.ask("Escolha uma opção", choices=["1", "0"], default="1")
            
            if choice == "0":
                return True
            elif choice == "1":
                register_api_key()
                
        else:
            # Exibir tabela simplificada
            _display_api_keys_table(keys)
            
            console.print("\n[bold]Opções:[/bold]")
            console.print("[1] Cadastrar Nova Key")
            console.print("[2] Editar Key")
            console.print("[3] Excluir Key")
            console.print("[0] Voltar")
            
            choice = Prompt.ask("Escolha uma opção", choices=["0", "1", "2", "3"], default="0")
            
            if choice == "0":
                return True
            elif choice == "1":
                register_api_key()
            elif choice == "2":
                edit_api_key(keys)
            elif choice == "3":
                _delete_api_key_interactive(keys)

def _display_api_keys_table(keys: List[APIKey]):
    """Helper para exibir tabela de keys"""
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("#", style="dim", width=3)
    table.add_column("Nome", style="cyan")
    table.add_column("Bridge Name", style="blue")
    table.add_column("Token (final)", style="yellow", width=12)
    table.add_column("Criado em", style="green")
    
    for i, key in enumerate(keys, 1):
        table.add_row(
            str(i),
            key.name,
            key.bridge_name or "-",
            key.get_masked_token(),
            key.get_formatted_created_at()
        )
    console.print(table)


def register_api_key() -> bool:
    """
    Registra uma nova API Key
    
    Returns:
        bool: True se registrou com sucesso, False caso contrário
    """
    logger.info("🔑 Iniciando cadastro de API Key")
    console.print("\n[bold blue]📝 Cadastrar API Key[/bold blue]")
    console.print("─" * 50)
    
    try:
        # Solicitar nome da API Key
        while True:
            name = Prompt.ask("Nome da API Key").strip()
            if not name:
                console.print("[red]❌ Nome não pode estar vazio[/red]")
                continue
            
            logger.debug(f"📝 Nome da API Key informado: {name}")
            
            # Verificar se já existe
            existing_key = secrets_store.get_key_by_name(name)
            if existing_key:
                logger.warning(f"⚠️ API Key com nome '{name}' já existe")
                console.print(f"[red]❌ Já existe uma API Key com o nome '{name}'[/red]")
                continue
            
            break
        
        # Solicitar token
        while True:
            token = Prompt.ask("Token").strip()
            if not token:
                console.print("[red]❌ Token não pode estar vazio[/red]")
                continue
            break
        
        # Solicitar bridge_name (opcional, mas recomendado)
        bridge_name = Prompt.ask("Nome do Bridge (ex: prod-db-01)").strip()
        if not bridge_name:
            bridge_name = None
        
        logger.debug(f"🔑 Token informado: {token[:10]}...")
        if bridge_name:
             logger.debug(f"🌉 Nome do Bridge: {bridge_name}")
        
        # Validar token
        console.print("\n[yellow]🔍 Validando token...[/yellow]")
        
        is_valid, message = http_client.validate_token(token)
        
        if not is_valid:
            logger.warning(f"❌ Falha na validação do token: {message}")
            console.print(f"[red]❌ {message}[/red]")
            console.print("[dim]Verifique se preencheu corretamente.[/dim]")
            console.print("\n[dim]Pressione Enter para continuar...[/dim]")
            input()
            return False
        
        # Salvar token
        logger.debug("💾 Salvando API Key no secrets store")
        console.print("[yellow]💾 Salvando...[/yellow]")
        secrets_store.add_key(name, token, bridge_name=bridge_name)
        
        logger.info(f"✅ API Key '{name}' cadastrada com sucesso")
        console.print(f"[green]✅ API Key cadastrada: {name}[/green]")
        if bridge_name:
            console.print(f"[dim]   Bridge Name: {bridge_name}[/dim]")
        return True
        
    except KeyboardInterrupt:
        logger.info("⚠️ Cadastro de API Key cancelado pelo usuário")
        console.print("\n[yellow]⚠️ Operação cancelada[/yellow]")
        return False
    except Exception as e:
        logger.exception(f"❌ Erro inesperado durante cadastro de API Key: {e}")
        console.print(f"[red]❌ Erro inesperado: {e}[/red]")
        return False
        console.print(f"[red]❌ Erro inesperado: {e}[/red]")
        return False


def edit_api_key(keys: List[APIKey]) -> bool:
    """
    Interface para editar uma API Key
    """
    console.print("\n[bold cyan]✏️ Editar API Key[/bold cyan]")
    
    # Selecionar Key
    while True:
        index_str = Prompt.ask("Número da Key para editar (0 para cancelar)").strip()
        if index_str == "0":
            return False
            
        try:
            index = int(index_str)
            if 1 <= index <= len(keys):
                break
            else:
                console.print(f"[red]❌ Número inválido[/red]")
        except ValueError:
             console.print("[red]❌ Digite um número válido[/red]")
             
    target_key = keys[index - 1]
    console.print(f"\nEditando: [bold]{target_key.name}[/bold]")
    console.print("[dim]Pressione Enter para manter o valor atual[/dim]")
    
    try:
        # Novo Nome (opcional)
        new_name = Prompt.ask(f"Novo Nome", default=target_key.name).strip()
        
        # Novo Bridge Name (opcional)
        current_bridge = target_key.bridge_name or ""
        # Logica: se user der enter, mantem default (current_bridge). 
        # Para limpar, user teria que digitar algo especifico? 
        # Vamos assumir que Enter mantem. Para limpar, user digita "none" ou "clear"? 
        # Simplificacao: Enter mantem. Espaço vazio mantem.
        new_bridge_name = Prompt.ask(f"Novo Bridge Name", default=current_bridge).strip()
        
        # Novo Token (opcional)
        new_token = Prompt.ask("Novo Token (deixe vazio para manter)", password=True).strip()
        
        # Confirmar
        if Confirm.ask("Salvar alterações?"):
            secrets_store.update_key(
                current_name=target_key.name,
                new_name=new_name if new_name != target_key.name else None,
                new_token=new_token if new_token else None,
                new_bridge_name=new_bridge_name if new_bridge_name != current_bridge else None
            )
            console.print("[green]✅ Key atualizada com sucesso![/green]")
            wait_for_continue()
            return True
            
    except Exception as e:
        console.print(f"[red]❌ Erro ao atualizar: {e}[/red]")
        wait_for_continue()
        
    return False

def _unused_list_api_keys() -> bool:
    """
    Lista todas as API Keys cadastradas
    
    Returns:
        bool: True se executou com sucesso, False caso contrário
    """
    logger.info("📋 Listando API Keys cadastradas")
    try:
        keys = secrets_store.list_keys()
        
        logger.debug(f"📊 Encontradas {len(keys)} API Keys")
        console.print(f"\n[bold blue]🔑 API Keys cadastradas ({len(keys)})[/bold blue]")
        console.print("─" * 60)
        
        if not keys:
            logger.debug("ℹ️ Nenhuma API Key encontrada")
            console.print("[dim]Nenhuma API Key cadastrada.[/dim]")
            return True
        
        # Criar tabela
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("#", style="dim", width=3)
        table.add_column("Nome", style="cyan")
        table.add_column("Bridge Name", style="blue")
        table.add_column("Token (final)", style="yellow", width=12)
        table.add_column("Criado em", style="green")
        
        for i, key in enumerate(keys, 1):
            table.add_row(
                str(i),
                key.name,
                key.bridge_name or "-",
                key.get_masked_token(),
                key.get_formatted_created_at()
            )
        
        console.print(table)
        
        # Opções de ação
        console.print("\n[bold]Opções:[/bold]")
        console.print("[A] Excluir por número")
        console.print("[0] Voltar")
        
        while True:
            choice = Prompt.ask("Escolha uma opção", default="0").strip().upper()
            
            logger.debug(f"🎯 Opção selecionada: {choice}")
            
            if choice == "0":
                logger.debug("↩️ Retornando ao menu principal")
                return True
            elif choice == "A":
                logger.debug("🗑️ Iniciando exclusão interativa de API Key")
                return _delete_api_key_interactive(keys)
            else:
                logger.warning(f"⚠️ Opção inválida selecionada: {choice}")
                console.print("[red]❌ Opção inválida[/red]")
        
    except Exception as e:
        logger.exception(f"❌ Erro ao listar API Keys: {e}")
        console.print(f"[red]❌ Erro ao listar API Keys: {e}[/red]")
        return False


def _delete_api_key_interactive(keys: List[APIKey]) -> bool:
    """
    Interface interativa para excluir API Key
    
    Args:
        keys: Lista de API Keys disponíveis
        
    Returns:
        bool: True se executou com sucesso, False caso contrário
    """
    try:
        while True:
            index_str = Prompt.ask("Número para excluir (0 para cancelar)").strip()
            
            if index_str == "0":
                return True
            
            try:
                index = int(index_str)
                if 1 <= index <= len(keys):
                    break
                else:
                    console.print(f"[red]❌ Número deve estar entre 1 e {len(keys)}[/red]")
            except ValueError:
                console.print("[red]❌ Digite um número válido[/red]")
        
        # Confirmar exclusão
        key_to_delete = keys[index - 1]
        confirmed = Confirm.ask(f"Confirmar exclusão de '{key_to_delete.name}'?", default=False)
        
        if confirmed:
            success = secrets_store.delete_key(key_to_delete.name)
            if success:
                console.print("[green]✅ Excluída.[/green]")
            else:
                console.print("[red]❌ Erro ao excluir[/red]")
        else:
            console.print("[yellow]⚠️ Exclusão cancelada[/yellow]")
        
        return True
        
    except Exception as e:
        console.print(f"[red]❌ Erro ao excluir API Key: {e}[/red]")
        return False


def list_schemas() -> bool:
    """
    Lista os modelos de dados (schemas) da DataSnap
    
    Returns:
        bool: True se executou com sucesso, False caso contrário
    """
    try:
        keys = secrets_store.list_keys()
        
        console.print("\n[bold blue]📊 Listar Modelos de Dados (Schemas)[/bold blue]")
        console.print("─" * 60)
        
        if not keys:
            console.print("[red]❌ Nenhuma API Key cadastrada.[/red]")
            console.print("[dim]Cadastre uma API Key primeiro para consultar schemas.[/dim]")
            return True
        
        # Mostrar lista de API Keys para escolher
        console.print("[bold]Escolha a API Key:[/bold]")
        
        for i, key in enumerate(keys, 1):
            console.print(f"[{i}] {key.name}")
        
        console.print("[0] Voltar")
        
        while True:
            choice = Prompt.ask("Escolha uma opção", default="0").strip()
            
            if choice == "0":
                return True
            
            try:
                index = int(choice)
                if 1 <= index <= len(keys):
                    selected_key = keys[index - 1]
                    return _fetch_and_display_schemas(selected_key)
                else:
                    console.print(f"[red]❌ Número deve estar entre 1 e {len(keys)}[/red]")
            except ValueError:
                console.print("[red]❌ Digite um número válido[/red]")
        
    except Exception as e:
        console.print(f"[red]❌ Erro ao listar schemas: {e}[/red]")
        return False


def _fetch_and_display_schemas(api_key: APIKey) -> bool:
    """
    Busca e exibe os schemas usando uma API Key específica
    
    Args:
        api_key: API Key para usar na requisição
        
    Returns:
        bool: True se executou com sucesso, False caso contrário
    """
    try:
        console.print(f"\n[yellow]🔍 GET /v1/schemas — usando '{api_key.name}'[/yellow]")
        console.print("─" * 60)
        
        # Fazer requisição
        success, data = http_client.get_schemas(api_key.token)
        
        if not success:
            console.print(f"[red]❌ {data}[/red]")
            return True
        
        # Verificar se a resposta tem o formato esperado
        if 'data' not in data:
            console.print("[red]❌ Formato de resposta inválido - 'data' não encontrado[/red]")
            return True
        
        schemas = data['data']
        
        if not schemas:
            console.print("[yellow]⚠️ Nenhum schema encontrado[/yellow]")
            return True
        
        # Criar tabela formatada
        table = Table(title="[bold green]Schemas DataSnap[/bold green]")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Slug", style="magenta")
        table.add_column("Nome", style="green")
        
        # Adicionar schemas à tabela
        for schema in schemas:
            schema_id = str(schema.get('id', 'N/A'))
            schema_slug = schema.get('slug', 'N/A')
            schema_name = schema.get('name', 'Sem nome')
            
            table.add_row(schema_id, schema_slug, schema_name)
        
        console.print(table)
        console.print(f"\n[dim]Total: {len(schemas)} schema(s) encontrado(s)[/dim]")
        
        # Opção para voltar
        console.print("\n[0] Voltar")
        Prompt.ask("Pressione Enter para continuar", default="")
        
        return True
        
    except Exception as e:
        console.print(f"[red]❌ Erro ao buscar schemas: {e}[/red]")
        return True


def show_statistics() -> None:
    """
    Exibe estatísticas básicas do sistema sem validar API
    """
    logger.info("📊 Exibindo estatísticas do sistema")
    try:
        # Contagem de API Keys
        keys_count = secrets_store.get_keys_count()
        
        # Contagem de fontes de dados
        from core.datasources_store import datasources_store
        datasources_count = len(datasources_store.list_datasources())
        
        # Contagem de fontes com tabelas configuradas
        datasources_with_tables = len([ds for ds in datasources_store.list_datasources() if ds.tables.selected])
        
        logger.debug(f"📈 Estatísticas: {keys_count} API Keys, {datasources_count} fontes de dados")
        console.print(f"\n[dim]📊 Estatísticas:[/dim]")
        console.print(f"[dim]   • API Keys cadastradas: {keys_count}[/dim]")
        console.print(f"[dim]   • Fontes de dados: {datasources_count}[/dim]")
        console.print(f"[dim]   • Fontes com tabelas: {datasources_with_tables}[/dim]")
        
        # Verificar se existe cache de schemas (sem fazer chamada à API)
        try:
            import os
            from core.paths import get_bridge_directory
            schemas_cache_path = os.path.join(get_bridge_directory(), "cache", "schemas.json")
            if os.path.exists(schemas_cache_path):
                with open(schemas_cache_path, 'r', encoding='utf-8') as f:
                    import json
                    cache_data = json.load(f)
                    schemas_count = len(cache_data.get('schemas', []))
                    console.print(f"[dim]   • Schemas em cache: {schemas_count}[/dim]")
        except Exception:
            # Ignorar erros de cache
            pass
        
        # Exibir fluxo de dados (mapeamentos)
        _show_data_flow()
        
    except Exception as e:
        logger.exception(f"❌ Erro ao exibir estatísticas: {e}")
        # Ignorar erros de estatísticas
        pass


def _show_data_flow() -> None:
    """
    Exibe o fluxo visual de dados (mapeamentos entre fontes e schemas)
    """
    try:
        import os
        import glob
        from core.paths import get_bridge_config_dir
        
        mappings_dir = os.path.join(get_bridge_config_dir(), "config", "mappings")
        
        if not os.path.exists(mappings_dir):
            return
        
        # Buscar todos os arquivos de mapeamento
        mapping_files = glob.glob(os.path.join(mappings_dir, "*.json"))
        
        if not mapping_files:
            return
        
        console.print(f"\n[bold cyan]🔄 Fluxo de Dados:[/bold cyan]")
        
        # Cache de schemas para evitar múltiplas requisições
        schemas_cache = {}
        
        def get_schema_info(schema_id):
            """Obtém informações do schema (slug) via API"""
            if schema_id in schemas_cache:
                return schemas_cache[schema_id]
            
            try:
                # Tentar obter uma API key válida
                api_keys = secrets_store.get_api_keys()
                if not api_keys:
                    return None
                
                # Usar a primeira API key disponível
                api_key = api_keys[0]
                success, data = http_client.get_schemas(api_key.token)
                
                if success and 'data' in data:
                    for schema in data['data']:
                        if str(schema.get('id')) == str(schema_id):
                            schema_info = {
                                'slug': schema.get('slug', 'N/A'),
                                'name': schema.get('name', 'Desconhecido')
                            }
                            schemas_cache[schema_id] = schema_info
                            return schema_info
                
                return None
            except Exception:
                return None
        
        # Agrupar mapeamentos por fonte de dados
        mappings_by_source = {}
        
        for mapping_file in mapping_files:
            try:
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    mapping_data = json.load(f)
                
                source_name = mapping_data.get('source', {}).get('name', 'Desconhecido')
                source_type = mapping_data.get('source', {}).get('type', 'Desconhecido')
                table_name = mapping_data.get('table', 'Desconhecida')
                schema_name = mapping_data.get('schema', {}).get('name', 'Desconhecido')
                schema_id = mapping_data.get('schema', {}).get('id', 'N/A')
                
                # Priorizar slug salva no arquivo de mapeamento
                schema_slug = mapping_data.get('schema', {}).get('slug')
                
                # Se não tiver slug no arquivo, tentar obter via API
                if not schema_slug:
                    schema_info = get_schema_info(schema_id)
                    schema_slug = schema_info.get('slug', 'N/A') if schema_info else 'N/A'
                
                if source_name not in mappings_by_source:
                    mappings_by_source[source_name] = {
                        'type': source_type,
                        'mappings': []
                    }
                
                mappings_by_source[source_name]['mappings'].append({
                    'table': table_name,
                    'schema_name': schema_name,
                    'schema_id': schema_id,
                    'schema_slug': schema_slug
                })
                
            except Exception as e:
                logger.warning(f"Erro ao ler mapeamento {mapping_file}: {e}")
                continue
        
        # Exibir fluxo visual para cada fonte de dados
        for source_name, source_data in mappings_by_source.items():
            source_type = source_data['type'].upper()
            console.print(f"\n[yellow]📊 Fonte:[/yellow] [bold]{source_name}[/bold] [dim]({source_type})[/dim]")
            
            for mapping in source_data['mappings']:
                # Desenhar fluxo visual com slug
                schema_display = f"{mapping['schema_name']} [dim](ID: {mapping['schema_id']}, Slug: {mapping['schema_slug']})[/dim]"
                console.print(f"   [cyan]├─[/cyan] [white]{mapping['table']}[/white] [dim]→[/dim] [green]{schema_display}[/green]")
        
        # Resumo
        total_mappings = sum(len(source_data['mappings']) for source_data in mappings_by_source.values())
        console.print(f"\n[dim]   • Total de vínculos: {total_mappings}[/dim]")
        console.print(f"[dim]   • Fontes vinculadas: {len(mappings_by_source)}[/dim]")
        
    except Exception as e:
        logger.warning(f"Erro ao exibir fluxo de dados: {e}")
        # Não exibir erro para o usuário, apenas log