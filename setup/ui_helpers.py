"""
Funções auxiliares para interface de usuário do setup
"""

from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.table import Table
from typing import List, Dict, Any, Optional
import math

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


def show_paginated_table(
    items: List[Dict[str, Any]], 
    columns: List[Dict[str, str]], 
    title: str = "Dados",
    items_per_page: int = 10,
    current_page: int = 1
) -> Dict[str, Any]:
    """
    Exibe uma tabela paginada com os dados fornecidos
    
    Args:
        items: Lista de itens para exibir
        columns: Lista de dicionários com 'key' e 'title' para as colunas
        title: Título da tabela
        items_per_page: Número de itens por página
        current_page: Página atual (1-indexed)
    
    Returns:
        Dict com informações de paginação: total_pages, current_page, has_next, has_prev
    """
    if not items:
        show_info_message("Nenhum item encontrado")
        return {"total_pages": 0, "current_page": 0, "has_next": False, "has_prev": False}
    
    # Calcular paginação
    total_items = len(items)
    total_pages = math.ceil(total_items / items_per_page)
    current_page = max(1, min(current_page, total_pages))
    
    start_idx = (current_page - 1) * items_per_page
    end_idx = min(start_idx + items_per_page, total_items)
    page_items = items[start_idx:end_idx]
    
    # Criar tabela
    table = Table(title=f"{title} (Página {current_page}/{total_pages})")
    
    # Adicionar colunas
    table.add_column("#", style="dim", width=4)
    for col in columns:
        table.add_column(col["title"], style="cyan")
    
    # Adicionar linhas
    for i, item in enumerate(page_items, start=start_idx + 1):
        row = [str(i)]
        for col in columns:
            value = item.get(col["key"], "N/A")
            # Truncar valores muito longos
            if isinstance(value, str) and len(value) > 30:
                value = value[:27] + "..."
            row.append(str(value))
        table.add_row(*row)
    
    console.print(table)
    
    # Mostrar informações de paginação se houver múltiplas páginas
    if total_pages > 1:
        pagination_info = []
        if current_page > 1:
            pagination_info.append("[p] Página anterior")
        if current_page < total_pages:
            pagination_info.append("[n] Próxima página")
        
        if pagination_info:
            show_info_message(f"Navegação: {' | '.join(pagination_info)}")
    
    return {
        "total_pages": total_pages,
        "current_page": current_page,
        "has_next": current_page < total_pages,
        "has_prev": current_page > 1,
        "total_items": total_items
    }


def show_simple_table(items: List[Dict[str, Any]], columns: List[Dict[str, str]], title: str = "Dados") -> None:
    """
    Exibe uma tabela simples sem paginação
    
    Args:
        items: Lista de itens para exibir
        columns: Lista de dicionários com 'key' e 'title' para as colunas
        title: Título da tabela
    """
    if not items:
        show_info_message("Nenhum item encontrado")
        return
    
    # Criar tabela
    table = Table(title=title)
    
    # Adicionar colunas
    table.add_column("#", style="dim", width=4)
    for col in columns:
        table.add_column(col["title"], style="cyan")
    
    # Adicionar linhas
    for i, item in enumerate(items, 1):
        row = [str(i)]
        for col in columns:
            value = item.get(col["key"], "N/A")
            # Truncar valores muito longos
            if isinstance(value, str) and len(value) > 30:
                value = value[:27] + "..."
            row.append(str(value))
        table.add_row(*row)
    
    console.print(table)