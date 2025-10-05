"""
Menu interativo TUI para o comando bridge setup
"""

from typing import Dict, Callable
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.text import Text

from core.secrets_store import secrets_store
from core.datasources_store import datasources_store
from core.logger import logger
from setup.actions import register_api_key, list_api_keys, list_schemas, show_statistics
from setup.datasources_menu import run_datasources_menu


from setup.ui_helpers import (
    show_success_message, show_error_message, show_warning_message, 
    show_info_message, wait_for_continue, show_header, show_separator
)


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
        
        # Exibir cabeçalho
        show_header()
        
        # Exibir estatísticas rápidas
        _show_quick_stats()
        
        # Exibir opções do menu
        console.print("\n📋 [bold]Opções disponíveis:[/bold]")
        console.print("  [cyan]1.[/cyan] Cadastrar/Listar API Keys")
        console.print("  [cyan]2.[/cyan] Listar Modelos de Dados (Schemas)")
        console.print("  [cyan]3.[/cyan] Fontes de Dados")
        console.print("  [cyan]0.[/cyan] Sair")
        
        # Obter escolha do usuário
        choice = Prompt.ask(
            "\n🎯 Escolha uma opção",
            choices=["0", "1", "2", "3"],
            default="0"
        )
        
        # Menu de opções
        menu_options: Dict[str, Callable[[], bool]] = {
            "1": register_api_key,
            "2": list_schemas,
            "3": run_datasources_menu,
            "0": lambda: False  # Sair
        }
        
        while True:
            
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
                        wait_for_continue()
                    
                    return True  # Continuar no menu
                    
                except Exception as e:
                    logger.exception(f"❌ Erro na operação do menu {choice}: {e}")
                    console.print(f"[red]❌ Erro na operação: {e}[/red]")
                    wait_for_continue()
                    return True
            else:
                logger.warning(f"⚠️ Opção inválida selecionada: {choice}")
                console.print("[red]❌ Opção inválida. Escolha 0, 1, 2, 3 ou 4.[/red]")
        
    except KeyboardInterrupt:
        logger.debug("⚠️ Menu principal interrompido pelo usuário")
        return False
    except Exception as e:
        logger.exception(f"❌ Erro no menu principal: {e}")
        console.print(f"[red]❌ Erro no menu: {e}[/red]")
        return False


def _show_quick_stats() -> None:
    """
    Exibe estatísticas rápidas do sistema
    """
    show_statistics()


# Remover as funções duplicadas que agora estão em ui_helpers