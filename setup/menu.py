"""
Menu interativo TUI para o comando bridge setup
"""

from typing import Dict, Callable
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.text import Text

from core.secrets_store import secrets_store
from core.logger import logger
from setup.actions import register_api_key, list_api_keys, list_schemas, show_statistics


console = Console()


def run_setup_menu() -> None:
    """
    Executa o menu principal do setup
    """
    logger.info("🚀 Iniciando menu de setup do DataSnap Bridge")
    try:
        # Inicializar o sistema
        _initialize_system()
        
        # Loop principal do menu
        while True:
            if not _show_main_menu():
                logger.info("👋 Saindo do menu de setup")
                break
                
    except KeyboardInterrupt:
        logger.info("⚠️ Menu de setup interrompido pelo usuário")
        console.print("\n[yellow]👋 Saindo...[/yellow]")
    except Exception as e:
        logger.exception(f"❌ Erro inesperado no menu de setup: {e}")
        console.print(f"\n[red]❌ Erro inesperado: {e}[/red]")


def _initialize_system() -> None:
    """
    Inicializa o sistema criando diretórios necessários
    """
    try:
        # Garantir que os diretórios existam
        from core.paths import ensure_bridge_directory
        ensure_bridge_directory()
        
        # Carregar secrets store
        secrets_store.load()
        
    except Exception as e:
        console.print(f"[red]❌ Erro ao inicializar sistema: {e}[/red]")
        raise


def _show_main_menu() -> bool:
    """
    Exibe o menu principal e processa a escolha do usuário
    
    Returns:
        bool: True para continuar, False para sair
    """
    try:
        # Limpar tela (opcional)
        console.clear()
        
        # Cabeçalho
        title = Text("Bridge Setup", style="bold blue")
        header = Panel(
            title,
            subtitle="DataSnap API Management",
            border_style="blue",
            padding=(1, 2)
        )
        console.print(header)
        
        # Estatísticas rápidas
        show_statistics()
        
        # Contar API Keys para exibir no menu
        keys_count = secrets_store.get_keys_count()
        
        # Opções do menu
        console.print("\n[bold]Menu Principal:[/bold]")
        console.print("[1] 📝 Cadastrar API Key")
        console.print(f"[2] 🔑 Listar API Keys ({keys_count})")
        console.print("[3] 📊 Listar Modelos de Dados (Schemas)")
        console.print("[0] 🚪 Sair")
        
        # Menu de opções
        menu_options: Dict[str, Callable[[], bool]] = {
            "1": register_api_key,
            "2": list_api_keys,
            "3": list_schemas,
            "0": lambda: False  # Sair
        }
        
        while True:
            choice = Prompt.ask("\nEscolha uma opção", default="0").strip()
            
            logger.debug(f"🎯 Opção selecionada no menu principal: {choice}")
            
            if choice in menu_options:
                if choice == "0":
                    logger.debug("🚪 Usuário escolheu sair do menu")
                    return False  # Sair
                
                # Executar ação
                try:
                    logger.debug(f"🔄 Executando ação do menu: {choice}")
                    result = menu_options[choice]()
                    
                    # Aguardar antes de voltar ao menu
                    if result:
                        _wait_for_continue()
                    
                    return True  # Continuar no menu
                    
                except Exception as e:
                    logger.exception(f"❌ Erro na operação do menu {choice}: {e}")
                    console.print(f"[red]❌ Erro na operação: {e}[/red]")
                    _wait_for_continue()
                    return True
            else:
                logger.warning(f"⚠️ Opção inválida selecionada: {choice}")
                console.print("[red]❌ Opção inválida. Escolha 0, 1, 2 ou 3.[/red]")
        
    except KeyboardInterrupt:
        logger.debug("⚠️ Menu principal interrompido pelo usuário")
        return False
    except Exception as e:
        logger.exception(f"❌ Erro no menu principal: {e}")
        console.print(f"[red]❌ Erro no menu: {e}[/red]")
        return False


def _wait_for_continue() -> None:
    """
    Aguarda o usuário pressionar Enter para continuar
    """
    try:
        console.print("\n[dim]Pressione Enter para voltar ao menu...[/dim]")
        input()
    except KeyboardInterrupt:
        pass


def _show_header() -> None:
    """
    Exibe o cabeçalho do aplicativo
    """
    header_text = """
╔══════════════════════════════════════╗
║            Bridge Setup              ║
║        DataSnap API Manager          ║
╚══════════════════════════════════════╝
    """
    console.print(header_text, style="bold blue")


def _show_separator(title: str = "") -> None:
    """
    Exibe um separador visual
    
    Args:
        title: Título opcional para o separador
    """
    if title:
        console.print(f"\n[bold]{title}[/bold]")
        console.print("─" * len(title))
    else:
        console.print("─" * 50)


# Funções auxiliares para melhorar a UX
def show_success_message(message: str) -> None:
    """
    Exibe uma mensagem de sucesso formatada
    
    Args:
        message: Mensagem a ser exibida
    """
    panel = Panel(
        f"✅ {message}",
        border_style="green",
        padding=(0, 1)
    )
    console.print(panel)


def show_error_message(message: str) -> None:
    """
    Exibe uma mensagem de erro formatada
    
    Args:
        message: Mensagem a ser exibida
    """
    panel = Panel(
        f"❌ {message}",
        border_style="red",
        padding=(0, 1)
    )
    console.print(panel)


def show_warning_message(message: str) -> None:
    """
    Exibe uma mensagem de aviso formatada
    
    Args:
        message: Mensagem a ser exibida
    """
    panel = Panel(
        f"⚠️ {message}",
        border_style="yellow",
        padding=(0, 1)
    )
    console.print(panel)


def show_info_message(message: str) -> None:
    """
    Exibe uma mensagem informativa formatada
    
    Args:
        message: Mensagem a ser exibida
    """
    panel = Panel(
        f"ℹ️ {message}",
        border_style="blue",
        padding=(0, 1)
    )
    console.print(panel)