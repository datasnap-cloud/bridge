#!/usr/bin/env python3
"""
CLI principal do DataSnap Bridge
"""

import typer
from rich.console import Console

app = typer.Typer(
    name="bridge",
    help="DataSnap Bridge - Ferramenta de linha de comando para DataSnap",
    add_completion=False
)

console = Console()


@app.command()
def setup():
    """
    Menu interativo para configurar API Keys e consultar Schemas da DataSnap
    
    Este comando abre um menu TUI (Terminal User Interface) que permite:
    - Cadastrar e validar API Keys da DataSnap
    - Listar API Keys cadastradas (com tokens mascarados)
    - Consultar Modelos de Dados (Schemas) da API
    - Gerenciar configurações de forma segura
    
    Todos os dados sensíveis são criptografados e armazenados localmente.
    """
    try:
        from setup.menu import run_setup_menu
        run_setup_menu()
    except KeyboardInterrupt:
        console.print("\n[yellow]👋 Saindo...[/yellow]")
    except ImportError as e:
        console.print(f"[red]❌ Erro ao importar módulos: {e}[/red]")
        console.print("[dim]Verifique se todas as dependências estão instaladas.[/dim]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]❌ Erro inesperado: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def version():
    """
    Exibe informações sobre a versão do Bridge
    """
    console.print("[bold blue]DataSnap Bridge v0.1.0[/bold blue]")
    console.print("[dim]Ferramenta de linha de comando para DataSnap API[/dim]")


@app.command()
def status():
    """
    Exibe o status do sistema e conectividade
    """
    try:
        from core.http import http_client
        from core.secrets_store import secrets_store
        
        console.print("[bold blue]📊 Status do Sistema[/bold blue]")
        console.print("─" * 30)
        
        # Carregar secrets store
        secrets_store.load()
        keys_count = secrets_store.get_keys_count()
        console.print(f"API Keys cadastradas: [cyan]{keys_count}[/cyan]")
        
        # Testar conectividade
        success, message = http_client.test_connection()
        status_color = "green" if success else "red"
        status_icon = "✅" if success else "❌"
        console.print(f"Conectividade API: [{status_color}]{status_icon} {message}[/{status_color}]")
        
    except Exception as e:
        console.print(f"[red]❌ Erro ao verificar status: {e}[/red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    try:
        app()
    except KeyboardInterrupt:
        console.print("\n[yellow]👋 Saindo...[/yellow]")
        raise typer.Exit(0)