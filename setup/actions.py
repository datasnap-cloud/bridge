"""
Módulo com ações do menu de setup
"""

import json
from typing import List, Optional, Tuple
from rich.console import Console
from rich.table import Table
from rich.syntax import Syntax
from rich.panel import Panel
from rich.prompt import Prompt, Confirm

from core.secrets_store import secrets_store, APIKey
from core.http import http_client


console = Console()


def register_api_key() -> bool:
    """
    Registra uma nova API Key
    
    Returns:
        bool: True se registrou com sucesso, False caso contrário
    """
    console.print("\n[bold blue]📝 Cadastrar API Key[/bold blue]")
    console.print("─" * 50)
    
    try:
        # Solicitar nome da API Key
        while True:
            name = Prompt.ask("Nome da API Key").strip()
            if not name:
                console.print("[red]❌ Nome não pode estar vazio[/red]")
                continue
            
            # Verificar se já existe
            existing_key = secrets_store.get_key_by_name(name)
            if existing_key:
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
        
        # Validar token
        console.print("\n[yellow]🔍 Validando token...[/yellow]")
        
        is_valid, message = http_client.validate_token(token)
        
        if not is_valid:
            console.print(f"[red]❌ {message}[/red]")
            console.print("[dim]Verifique se preencheu corretamente.[/dim]")
            return False
        
        # Salvar token
        console.print("[yellow]💾 Salvando...[/yellow]")
        secrets_store.add_key(name, token)
        
        console.print(f"[green]✅ API Key cadastrada: {name}[/green]")
        return True
        
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ Operação cancelada[/yellow]")
        return False
    except Exception as e:
        console.print(f"[red]❌ Erro inesperado: {e}[/red]")
        return False


def list_api_keys() -> bool:
    """
    Lista todas as API Keys cadastradas
    
    Returns:
        bool: True se executou com sucesso, False caso contrário
    """
    try:
        keys = secrets_store.list_keys()
        
        console.print(f"\n[bold blue]🔑 API Keys cadastradas ({len(keys)})[/bold blue]")
        console.print("─" * 60)
        
        if not keys:
            console.print("[dim]Nenhuma API Key cadastrada.[/dim]")
            return True
        
        # Criar tabela
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("#", style="dim", width=3)
        table.add_column("Nome", style="cyan")
        table.add_column("Token (final)", style="yellow", width=12)
        table.add_column("Criado em", style="green")
        
        for i, key in enumerate(keys, 1):
            table.add_row(
                str(i),
                key.name,
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
            
            if choice == "0":
                return True
            elif choice == "A":
                return _delete_api_key_interactive(keys)
            else:
                console.print("[red]❌ Opção inválida[/red]")
        
    except Exception as e:
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
        
        # Exibir JSON formatado e colorido
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        syntax = Syntax(json_str, "json", theme="monokai", line_numbers=True)
        
        panel = Panel(
            syntax,
            title="[bold green]Schemas DataSnap[/bold green]",
            border_style="green"
        )
        
        console.print(panel)
        
        # Opção para voltar
        console.print("\n[0] Voltar")
        Prompt.ask("Pressione Enter para continuar", default="")
        
        return True
        
    except Exception as e:
        console.print(f"[red]❌ Erro ao buscar schemas: {e}[/red]")
        return True


def show_statistics() -> None:
    """
    Exibe estatísticas básicas do sistema
    """
    try:
        keys_count = secrets_store.get_keys_count()
        
        console.print(f"\n[dim]📊 Estatísticas:[/dim]")
        console.print(f"[dim]   • API Keys cadastradas: {keys_count}[/dim]")
        
        # Só testar conectividade se houver chaves cadastradas
        if keys_count > 0:
            success, message = http_client.test_connection()
            status_color = "green" if success else "red"
            status_icon = "✅" if success else "❌"
            console.print(f"[dim]   • Conectividade API: [{status_color}]{status_icon} {message}[/{status_color}][/dim]")
        else:
            console.print(f"[dim]   • Conectividade API: [yellow]⏸️ Cadastre uma API Key primeiro[/yellow][/dim]")
        
    except Exception:
        # Ignorar erros de estatísticas
        pass