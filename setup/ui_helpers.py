"""
Funções auxiliares para interface de usuário do setup
"""

from rich.console import Console
from rich.panel import Panel
from rich.text import Text

console = Console()


def show_success_message(message: str) -> None:
    """
    Exibe uma mensagem de sucesso formatada
    
    Args:
        message: Mensagem a ser exibida
    """
    text = Text(f"✅ {message}", style="bold green")
    panel = Panel(
        text,
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
    text = Text(f"❌ {message}", style="bold red")
    panel = Panel(
        text,
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
    text = Text(f"⚠️ {message}", style="bold yellow")
    panel = Panel(
        text,
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
    text = Text(f"ℹ️ {message}", style="bold blue")
    panel = Panel(
        text,
        border_style="blue",
        padding=(0, 1)
    )
    console.print(panel)


def wait_for_continue() -> None:
    """
    Pausa a execução aguardando o usuário pressionar Enter
    """
    try:
        input("\n📎 Pressione Enter para continuar...")
    except KeyboardInterrupt:
        # Graceful exit on Ctrl+C
        pass


def show_header() -> None:
    """
    Exibe o cabeçalho do menu
    """
    header_text = Text("🌉 DataSnap Bridge - Setup", style="bold cyan")
    panel = Panel(
        header_text,
        border_style="cyan",
        padding=(1, 2)
    )
    console.print(panel)


def show_separator(title: str = "") -> None:
    """
    Exibe um separador visual
    
    Args:
        title: Título opcional para o separador
    """
    if title:
        text = Text(f"─── {title} ───", style="dim")
    else:
        text = Text("─" * 50, style="dim")
    
    console.print(text)