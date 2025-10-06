#!/usr/bin/env python3
"""
CLI principal do DataSnap Bridge
"""

import asyncio
import typer
from rich.console import Console
from rich.table import Table
from typing import List, Optional

from core.logger import logger

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
    logger.info("🚀 Iniciando comando setup")
    
    try:
        from setup.menu import run_setup_menu
        logger.debug("📋 Carregando menu de setup")
        run_setup_menu()
        logger.info("✅ Comando setup finalizado com sucesso")
    except KeyboardInterrupt:
        logger.info("⏹️ Setup interrompido pelo usuário")
        console.print("\n[yellow]👋 Saindo...[/yellow]")
    except ImportError as e:
        logger.error(f"📦 Erro ao importar módulos: {e}")
        console.print(f"[red]❌ Erro ao importar módulos: {e}[/red]")
        console.print("[dim]Verifique se todas as dependências estão instaladas.[/dim]")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception(f"💥 Erro inesperado no setup: {e}")
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
def sync(
    mappings: Optional[List[str]] = typer.Option(None, "--mapping", "-m", help="Mapeamentos específicos para sincronizar"),
    all_mappings: bool = typer.Option(False, "--all", "-a", help="Sincronizar todos os mapeamentos"),
    parallel: bool = typer.Option(True, "--parallel/--sequential", help="Executar sincronizações em paralelo"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Simular execução sem fazer upload real"),
    force: bool = typer.Option(False, "--force", "-f", help="Forçar sincronização completa"),
    status_only: bool = typer.Option(False, "--status", help="Mostrar apenas o status das sincronizações"),
    max_workers: int = typer.Option(4, "--workers", "-w", help="Número máximo de workers paralelos"),
    batch_size: int = typer.Option(10000, "--batch-size", "-b", help="Tamanho do lote de registros"),
):
    """
    Sincroniza mapeamentos com a API DataSnap
    
    Este comando executa o pipeline de sincronização que:
    - Extrai dados das fontes configuradas
    - Converte para formato JSONL
    - Faz upload para a API DataSnap
    - Coleta métricas e mantém estado
    
    Exemplos:
    - bridge sync --all                    # Sincroniza todos os mapeamentos
    - bridge sync -m users -m products     # Sincroniza mapeamentos específicos
    - bridge sync --dry-run --all          # Simula sincronização de todos
    - bridge sync --status                 # Mostra status das sincronizações
    """
    logger.info("🔄 Iniciando comando sync")
    
    try:
        from sync.runner import SyncConfig, run_sync_command, create_sync_runner, format_sync_results
        
        if status_only:
            # Mostrar apenas status
            runner = create_sync_runner()
            status = runner.get_sync_status()
            _display_sync_status(status)
            return
        
        # Validar parâmetros
        if not all_mappings and not mappings:
            console.print("[red]❌ Especifique mapeamentos com --mapping ou use --all[/red]")
            raise typer.Exit(1)
        
        # Configurar sincronização
        config = SyncConfig(
            max_workers=max_workers,
            batch_size=batch_size,
            dry_run=dry_run,
            force_full_sync=force
        )
        
        # Executar sincronização
        console.print("[bold blue]🔄 Iniciando sincronização...[/bold blue]")
        if dry_run:
            console.print("[yellow]⚠️ Modo simulação ativado - nenhum upload será realizado[/yellow]")
        
        logger.info(f"[DEBUG] Prestes a chamar asyncio.run com mapping_names: {mappings}, all_mappings: {all_mappings}")
        
        results = asyncio.run(run_sync_command(
            mapping_names=mappings,
            all_mappings=all_mappings,
            parallel=parallel,
            dry_run=dry_run,
            force=force,
            config=config
        ))
        
        logger.info(f"[DEBUG] asyncio.run retornou com {len(results)} resultados")
        
        # Exibir resultados
        console.print("\n" + format_sync_results(results))
        
        # Verificar se houve falhas
        failed_count = len([r for r in results if not r.success])
        if failed_count > 0:
            logger.warning(f"Sincronização concluída com {failed_count} falhas")
            raise typer.Exit(1)
        else:
            logger.info("✅ Sincronização concluída com sucesso")
            
    except KeyboardInterrupt:
        logger.info("⏹️ Sincronização interrompida pelo usuário")
        console.print("\n[yellow]👋 Sincronização interrompida...[/yellow]")
        raise typer.Exit(130)
    except ImportError as e:
        logger.error(f"📦 Erro ao importar módulos de sincronização: {e}")
        console.print(f"[red]❌ Erro ao importar módulos: {e}[/red]")
        console.print("[dim]Verifique se todas as dependências estão instaladas.[/dim]")
        raise typer.Exit(1)
    except Exception as e:
        logger.exception(f"💥 Erro inesperado na sincronização: {e}")
        console.print(f"[red]❌ Erro inesperado: {e}[/red]")
        raise typer.Exit(1)


def _display_sync_status(status: dict):
    """Exibe o status das sincronizações em formato tabular."""
    console.print("[bold blue]📊 Status das Sincronizações[/bold blue]")
    console.print("─" * 50)
    
    # Informações gerais
    console.print(f"Total de mapeamentos: [cyan]{status['total_mappings']}[/cyan]")
    console.print(f"Sincronizações em execução: [yellow]{len(status['running_syncs'])}[/yellow]")
    
    if status['running_syncs']:
        console.print(f"Executando: [yellow]{', '.join(status['running_syncs'])}[/yellow]")
    
    # Tabela de últimas sincronizações
    if status['last_sync_times']:
        console.print("\n[bold]Últimas Sincronizações:[/bold]")
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Mapeamento", style="cyan")
        table.add_column("Última Sincronização", style="green")
        table.add_column("Total de Syncs", style="blue")
        table.add_column("Erros", style="red")
        
        for mapping in status['last_sync_times']:
            last_sync = status['last_sync_times'].get(mapping, "Nunca")
            sync_count = status['sync_counts'].get(mapping, 0)
            error_count = status['error_counts'].get(mapping, 0)
            
            table.add_row(
                mapping,
                str(last_sync),
                str(sync_count),
                str(error_count) if error_count > 0 else "0"
            )
        
        console.print(table)
    else:
        console.print("\n[dim]Nenhuma sincronização executada ainda.[/dim]")


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
        
        # Status das sincronizações
        try:
            from sync.runner import create_sync_runner
            runner = create_sync_runner()
            sync_status = runner.get_sync_status()
            console.print(f"Mapeamentos disponíveis: [cyan]{sync_status['total_mappings']}[/cyan]")
            console.print(f"Sincronizações ativas: [yellow]{len(sync_status['running_syncs'])}[/yellow]")
        except Exception:
            console.print("Status de sincronização: [dim]Não disponível[/dim]")
        
    except Exception as e:
        console.print(f"[red]❌ Erro ao verificar status: {e}[/red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    try:
        app()
    except KeyboardInterrupt:
        console.print("\n[yellow]👋 Saindo...[/yellow]")
        raise typer.Exit(0)