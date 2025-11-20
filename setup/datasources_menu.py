"""
Menu interativo para gerenciamento de Fontes de Dados
Suporta MySQL e PostgreSQL com interface de terminal
"""

import re
import os
import json
from typing import List, Optional, Tuple
from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.text import Text
from rich.table import Table

from core.datasources_store import datasources_store, DataSource, DatabaseConnection
from core.database_validators import DatabaseValidatorFactory, TableInfo
from core.database_connector import create_database_connector
from core.paths import get_bridge_config_dir
from core.logger import logger
from setup.ui_helpers import show_success_message, show_error_message, show_warning_message, show_info_message


console = Console()


def run_datasources_menu() -> bool:
    """
    Executa o menu de fontes de dados
    
    Returns:
        bool: True para continuar no menu principal, False para sair
    """
    logger.info("🗄️ Iniciando menu de fontes de dados")
    
    try:
        while True:
            if not _show_datasources_menu():
                return True  # Voltar ao menu principal
                
    except KeyboardInterrupt:
        logger.info("⚠️ Menu de fontes de dados interrompido pelo usuário")
        console.print("\n[yellow]👋 Voltando ao menu principal...[/yellow]")
        return True
    except Exception as e:
        logger.exception(f"❌ Erro inesperado no menu de fontes de dados: {e}")
        show_error_message(f"Erro inesperado: {e}")
        return True


def _show_datasources_menu() -> bool:
    """
    Exibe o menu de fontes de dados e processa a escolha do usuário
    
    Returns:
        bool: True para continuar, False para voltar ao menu principal
    """
    try:
        # Limpar tela
        console.clear()
        
        # Cabeçalho
        title = Text("Fontes de Dados", style="bold green")
        header = Panel(
            title,
            subtitle="Gerenciamento de Conexões MySQL e PostgreSQL",
            border_style="green",
            padding=(1, 2)
        )
        console.print(header)
        
        # Estatísticas
        _show_datasources_statistics()
        
        # Opções do menu
        console.print("\n[bold]Menu de Fontes de Dados:[/bold]")
        console.print("[1] ➕ Criar nova fonte de dados")
        console.print("[2] 📋 Listar fontes de dados")
        console.print("[3] 🗑️ Excluir fonte de dados")
        console.print("[4] 📊 Cadastrar tabelas de uma fonte")
        console.print("[5] 📄 Cadastrar fonte de log Laravel")
        console.print("[0] ⬅️ Voltar ao menu principal")
        
        while True:
            choice = Prompt.ask("\nEscolha uma opção", default="0").strip()
            
            logger.debug(f"🎯 Opção selecionada no menu de fontes de dados: {choice}")
            
            if choice == "0":
                logger.debug("⬅️ Usuário escolheu voltar ao menu principal")
                return False
            elif choice == "1":
                _create_new_datasource()
                return True
            elif choice == "2":
                _list_datasources()
                _wait_for_continue()
                return True
            elif choice == "3":
                _delete_datasource()
                return True
            elif choice == "4":
                _register_tables_menu()
                return True
            elif choice == "5":
                _create_laravel_log_datasource()
                return True
            else:
                logger.warning(f"⚠️ Opção inválida selecionada: {choice}")
                console.print("[red]❌ Opção inválida. Escolha 0, 1, 2, 3 ou 4.[/red]")
        
    except KeyboardInterrupt:
        return False
    except Exception as e:
        logger.exception(f"❌ Erro no menu de fontes de dados: {e}")
        show_error_message(f"Erro no menu: {e}")
        return False


def _show_datasources_statistics() -> None:
    """Exibe estatísticas das fontes de dados"""
    try:
        datasources = datasources_store.list_datasources()
        total = len(datasources)
        
        mysql_count = len([ds for ds in datasources if ds.type == 'mysql'])
        postgresql_count = len([ds for ds in datasources if ds.type == 'postgresql'])
        
        with_tables = len([ds for ds in datasources if ds.tables.selected])
        
        stats_text = f"📊 Total: {total} | MySQL: {mysql_count} | PostgreSQL: {postgresql_count} | Com tabelas: {with_tables}"
        
        panel = Panel(
            stats_text,
            border_style="dim",
            padding=(0, 1)
        )
        console.print(panel)
        
    except Exception as e:
        logger.warning(f"⚠️ Erro ao exibir estatísticas: {e}")


def _create_new_datasource() -> None:
    """Cria uma nova fonte de dados"""
    logger.debug("➕ Iniciando criação de nova fonte de dados")
    
    try:
        console.clear()
        
        # Cabeçalho
        title = Text("Criar Nova Fonte de Dados", style="bold cyan")
        header = Panel(title, border_style="cyan", padding=(1, 2))
        console.print(header)
        
        # Escolher tipo de banco
        db_type = _choose_database_type()
        if not db_type:
            return
        
        # Coletar dados de conexão
        conn_data = _collect_connection_data(db_type)
        if not conn_data:
            return
        
        name, host, port, database, user, password = conn_data
        
        # Criar objeto de conexão
        connection = DatabaseConnection(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            options=_get_default_options(db_type)
        )
        
        # Validar conexão
        console.print("\n[yellow]🔍 Validando conexão...[/yellow]")
        
        result = DatabaseValidatorFactory.validate_connection(
            db_type, host, port, database, user, password
        )
        
        if result.success:
            show_success_message(f"✅ {result.message} ({result.connection_time_ms}ms)")
            
            # Salvar fonte de dados
            try:
                datasource = datasources_store.add_datasource(name, db_type, connection)
                show_success_message(f"💾 Fonte de dados '{name}' salva com sucesso!")
                logger.info(f"✅ Fonte de dados criada: {name} ({db_type})")
                
            except ValueError as e:
                show_error_message(str(e))
                logger.warning(f"⚠️ Erro ao salvar fonte de dados: {e}")
            except Exception as e:
                show_error_message(f"Erro ao salvar: {e}")
                logger.exception(f"❌ Erro ao salvar fonte de dados: {e}")
        else:
            show_error_message(f"❌ {result.message}")
            if result.error_details:
                logger.warning(f"⚠️ Detalhes do erro: {result.error_details}")
            
            show_warning_message("A fonte de dados não foi salva.")
        
        _wait_for_continue()
        
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ Criação cancelada pelo usuário[/yellow]")
    except Exception as e:
        logger.exception(f"❌ Erro na criação de fonte de dados: {e}")
        show_error_message(f"Erro inesperado: {e}")
        _wait_for_continue()


def _create_laravel_log_datasource() -> None:
    logger.debug("➕ Iniciando criação de fonte de log Laravel")
    try:
        console.clear()
        title = Text("Criar Fonte de Log Laravel", style="bold cyan")
        header = Panel(title, border_style="cyan", padding=(1, 2))
        console.print(header)
        while True:
            name = Prompt.ask("Nome da fonte de log").strip()
            if not name:
                console.print("[red]❌ Nome não pode estar vazio.[/red]")
                continue
            if datasources_store.get_datasource_by_name(name):
                console.print(f"[red]❌ Já existe uma fonte com o nome '{name}'.[/red]")
                continue
            break
        log_path = Prompt.ask("Caminho do arquivo laravel.log").strip()
        if not log_path:
            show_error_message("Caminho do arquivo é obrigatório.")
            _wait_for_continue()
            return
        max_mb_str = Prompt.ask("Limite de memória em MB", default="50").strip()
        try:
            max_mb = int(max_mb_str)
        except ValueError:
            max_mb = 50
        connection = DatabaseConnection(
            host="local",
            port=0,
            database="laravel",
            user="",
            password="",
            options={
                "log_path": log_path,
                "max_memory_mb": max_mb
            }
        )
        try:
            datasource = datasources_store.add_datasource(name, "laravel_log", connection)
            show_success_message(f"💾 Fonte de log '{name}' salva: {log_path}")
            logger.info(f"✅ Fonte de log criada: {name} -> {log_path}")
        except ValueError as e:
            show_error_message(str(e))
            logger.warning(f"⚠️ Erro ao salvar fonte de log: {e}")
        except Exception as e:
            show_error_message(f"Erro ao salvar: {e}")
            logger.exception(f"❌ Erro ao salvar fonte de log: {e}")
        _wait_for_continue()
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ Criação cancelada pelo usuário[/yellow]")
    except Exception as e:
        logger.exception(f"❌ Erro na criação de fonte de log: {e}")
        show_error_message(f"Erro inesperado: {e}")
        _wait_for_continue()


def _choose_database_type() -> Optional[str]:
    """
    Permite ao usuário escolher o tipo de banco de dados
    
    Returns:
        Optional[str]: Tipo escolhido ('mysql' ou 'postgresql') ou None se cancelado
    """
    console.print("\n[bold]Escolha o tipo de banco de dados:[/bold]")
    console.print("[1] 🐬 MySQL")
    console.print("[2] 🐘 PostgreSQL")
    console.print("[0] ⬅️ Voltar")
    
    while True:
        choice = Prompt.ask("\nTipo de banco", default="0").strip()
        
        if choice == "0":
            return None
        elif choice == "1":
            return "mysql"
        elif choice == "2":
            return "postgresql"
        else:
            console.print("[red]❌ Opção inválida. Escolha 0, 1 ou 2.[/red]")


def _collect_connection_data(db_type: str) -> Optional[Tuple[str, str, int, str, str, str]]:
    """
    Coleta os dados de conexão do usuário
    
    Args:
        db_type: Tipo do banco ('mysql' ou 'postgresql')
        
    Returns:
        Optional[Tuple]: (nome, host, porta, database, usuário, senha) ou None se cancelado
    """
    try:
        console.print(f"\n[bold]Dados de Conexão - {db_type.upper()}:[/bold]")
        
        # Nome da fonte (único)
        while True:
            name = Prompt.ask("Nome da fonte de dados").strip()
            if not name:
                console.print("[red]❌ Nome não pode estar vazio.[/red]")
                continue
            
            # Verificar se nome já existe
            if datasources_store.get_datasource_by_name(name):
                console.print(f"[red]❌ Já existe uma fonte com o nome '{name}'. Escolha outro nome.[/red]")
                continue
            
            break
        
        # Host
        host = Prompt.ask("Host/Endereço", default="localhost").strip()
        if not host:
            host = "localhost"
        
        # Porta
        default_port = 3306 if db_type == "mysql" else 5432
        while True:
            port_str = Prompt.ask(f"Porta", default=str(default_port)).strip()
            try:
                port = int(port_str)
                if 1 <= port <= 65535:
                    break
                else:
                    console.print("[red]❌ Porta deve estar entre 1 e 65535.[/red]")
            except ValueError:
                console.print("[red]❌ Porta deve ser um número.[/red]")
        
        # Database
        database = Prompt.ask("Nome do banco de dados").strip()
        if not database:
            console.print("[red]❌ Nome do banco é obrigatório.[/red]")
            return None
        
        # Usuário
        user = Prompt.ask("Usuário").strip()
        if not user:
            console.print("[red]❌ Usuário é obrigatório.[/red]")
            return None
        
        # Senha (visível)
        password = Prompt.ask("Senha", password=False, default="").strip()
        
        return name, host, port, database, user, password
        
    except KeyboardInterrupt:
        return None


def _get_default_options(db_type: str) -> dict:
    """
    Retorna opções padrão para o tipo de banco
    
    Args:
        db_type: Tipo do banco
        
    Returns:
        dict: Opções padrão
    """
    if db_type == "mysql":
        return {
            "charset": "utf8mb4",
            "connect_timeout": 8
        }
    elif db_type == "postgresql":
        return {
            "connect_timeout": 8
        }
    else:
        return {}


def _list_datasources() -> None:
    """Lista todas as fontes de dados cadastradas"""
    logger.debug("📋 Listando fontes de dados")
    
    try:
        datasources = datasources_store.list_datasources()
        
        if not datasources:
            show_info_message("📭 Nenhuma fonte de dados cadastrada.")
            return
        
        console.print(f"\n[bold]📋 Fontes de Dados Cadastradas ({len(datasources)}):[/bold]")
        
        # Criar tabela
        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Nome", style="cyan", no_wrap=True)
        table.add_column("Tipo", style="green", no_wrap=True)
        table.add_column("Conexão", style="yellow")
        table.add_column("Tabelas", style="magenta", no_wrap=True)
        table.add_column("Criado em", style="dim", no_wrap=True)
        
        for datasource in datasources:
            table_count = len(datasource.tables.selected) if datasource.tables.selected else 0
            table_info = f"{table_count} tabelas" if table_count > 0 else "Nenhuma"
            
            table.add_row(
                datasource.name,
                datasource.type.upper(),
                datasource.get_connection_summary(),
                table_info,
                datasource.get_formatted_created_at()
            )
        
        console.print(table)
        
    except Exception as e:
        logger.exception(f"❌ Erro ao listar fontes de dados: {e}")
        show_error_message(f"Erro ao listar fontes: {e}")


def _delete_datasource() -> None:
    """Exclui uma fonte de dados"""
    logger.debug("🗑️ Iniciando exclusão de fonte de dados")
    
    try:
        datasources = datasources_store.list_datasources()
        
        if not datasources:
            show_info_message("📭 Nenhuma fonte de dados para excluir.")
            _wait_for_continue()
            return
        
        console.print(f"\n[bold]🗑️ Excluir Fonte de Dados:[/bold]")
        
        # Listar fontes disponíveis
        console.print("\n[bold]Fontes disponíveis:[/bold]")
        for i, datasource in enumerate(datasources, 1):
            table_count = len(datasource.tables.selected) if datasource.tables.selected else 0
            console.print(f"[{i}] {datasource.name} ({datasource.type.upper()}) - {datasource.get_connection_summary()} - {table_count} tabelas")
        
        console.print("[0] ⬅️ Voltar")
        
        while True:
            choice = Prompt.ask("\nEscolha a fonte para excluir", default="0").strip()
            
            if choice == "0":
                return
            
            try:
                index = int(choice) - 1
                if 0 <= index < len(datasources):
                    selected_datasource = datasources[index]
                    
                    # Confirmar exclusão
                    if Confirm.ask(f"\n[red]⚠️ Tem certeza que deseja excluir '{selected_datasource.name}'?[/red]"):
                        if datasources_store.delete_datasource(selected_datasource.name):
                            show_success_message(f"✅ Fonte '{selected_datasource.name}' excluída com sucesso!")
                            logger.info(f"🗑️ Fonte de dados excluída: {selected_datasource.name}")
                        else:
                            show_error_message("❌ Erro ao excluir fonte de dados.")
                    else:
                        show_info_message("ℹ️ Exclusão cancelada.")
                    
                    _wait_for_continue()
                    return
                else:
                    console.print("[red]❌ Opção inválida.[/red]")
            except ValueError:
                console.print("[red]❌ Digite um número válido.[/red]")
        
    except Exception as e:
        logger.exception(f"❌ Erro na exclusão de fonte de dados: {e}")
        show_error_message(f"Erro inesperado: {e}")
        _wait_for_continue()


def _register_tables_menu() -> None:
    """Menu para cadastrar tabelas de uma fonte"""
    logger.debug("📊 Iniciando menu de cadastro de tabelas")
    
    try:
        datasources = datasources_store.list_datasources()
        
        if not datasources:
            show_info_message("📭 Nenhuma fonte de dados disponível.")
            _wait_for_continue()
            return
        
        console.print(f"\n[bold]📊 Cadastrar Tabelas:[/bold]")
        
        # Listar fontes disponíveis
        console.print("\n[bold]Escolha a fonte de dados:[/bold]")
        for i, datasource in enumerate(datasources, 1):
            table_count = len(datasource.tables.selected) if datasource.tables.selected else 0
            console.print(f"[{i}] {datasource.name} ({datasource.type.upper()}) - {table_count} tabelas selecionadas")
        
        console.print("[0] ⬅️ Voltar")
        
        while True:
            choice = Prompt.ask("\nEscolha a fonte", default="0").strip()
            
            if choice == "0":
                return
            
            try:
                index = int(choice) - 1
                if 0 <= index < len(datasources):
                    selected_datasource = datasources[index]
                    _register_tables_for_datasource(selected_datasource)
                    return
                else:
                    console.print("[red]❌ Opção inválida.[/red]")
            except ValueError:
                console.print("[red]❌ Digite um número válido.[/red]")
        
    except Exception as e:
        logger.exception(f"❌ Erro no menu de cadastro de tabelas: {e}")
        show_error_message(f"Erro inesperado: {e}")
        _wait_for_continue()


def _register_tables_for_datasource(datasource: DataSource) -> None:
    """
    Cadastra tabelas para uma fonte de dados específica
    
    Args:
        datasource: Fonte de dados selecionada
    """
    logger.debug(f"📊 Cadastrando tabelas para: {datasource.name}")
    
    try:
        console.clear()
        
        # Cabeçalho
        title = Text(f"Cadastrar Tabelas - {datasource.name}", style="bold magenta")
        header = Panel(title, border_style="magenta", padding=(1, 2))
        console.print(header)
        
        # Descobrir tabelas
        console.print("[yellow]🔍 Descobrindo tabelas disponíveis...[/yellow]")
        
        success, tables, error_msg = DatabaseValidatorFactory.discover_tables(
            datasource.type,
            datasource.conn.host,
            datasource.conn.port,
            datasource.conn.database,
            datasource.conn.user,
            datasource.conn.password
        )
        
        if not success:
            show_error_message(f"❌ Erro ao descobrir tabelas: {error_msg}")
            _wait_for_continue()
            return
        
        if not tables:
            show_info_message("📭 Nenhuma tabela encontrada no banco de dados.")
            _wait_for_continue()
            return
        
        # Mostrar tabelas e permitir seleção
        selected_tables = _select_tables_interactive(tables, datasource.tables.selected)
        
        if selected_tables is not None:
            # Salvar seleção
            if datasources_store.save_selected_tables(datasource.name, selected_tables):
                show_success_message(f"✅ Seleção salva! {len(selected_tables)} tabelas selecionadas.")
                logger.info(f"📊 Tabelas salvas para {datasource.name}: {len(selected_tables)} tabelas")
                
                # Removido: opção de gerar modelos JSONL para as tabelas selecionadas
            else:
                show_error_message("❌ Erro ao salvar seleção de tabelas.")
        else:
            show_info_message("ℹ️ Seleção cancelada.")
        
        _wait_for_continue()
        
    except Exception as e:
        logger.exception(f"❌ Erro no cadastro de tabelas: {e}")
        show_error_message(f"Erro inesperado: {e}")
        _wait_for_continue()


def _select_tables_interactive(tables: List[TableInfo], current_selection: List[str]) -> Optional[List[str]]:
    """
    Interface interativa para seleção de tabelas
    
    Args:
        tables: Lista de tabelas disponíveis
        current_selection: Seleção atual
        
    Returns:
        Optional[List[str]]: Lista de tabelas selecionadas ou None se cancelado
    """
    selected = set(current_selection)
    
    while True:
        console.clear()
        
        # Cabeçalho
        console.print("[bold]📊 Seleção de Tabelas:[/bold]")
        console.print(f"Total disponível: {len(tables)} | Selecionadas: {len(selected)}")
        
        # Mostrar tabelas
        console.print("\n[bold]Tabelas disponíveis:[/bold]")
        for i, table in enumerate(tables, 1):
            status = "✅" if table.name in selected else "⬜"
            console.print(f"{status} [{i:2d}] {table.name}")
        
        # Mostrar opções
        console.print("\n[bold]Opções:[/bold]")
        console.print("• Digite números separados por vírgula (ex: 1,3,5-8)")
        console.print("• [T] Selecionar todas")
        console.print("• [L] Limpar seleção")
        console.print("• [S] Salvar e sair")
        console.print("• [E] Editar seleção atual")
        console.print("• [C] Cancelar")
        
        if selected:
            console.print(f"\n[green]Selecionadas: {', '.join(sorted(selected))}[/green]")
        
        choice = Prompt.ask("\nComando", default="S").strip().upper()
        
        if choice == "C":
            return None
        elif choice == "S":
            return list(selected)
        elif choice == "T":
            selected = {table.name for table in tables}
            show_success_message(f"✅ Todas as {len(tables)} tabelas selecionadas!")
        elif choice == "L":
            selected.clear()
            show_info_message("ℹ️ Seleção limpa.")
        elif choice == "E":
            _show_current_selection(selected)
            _wait_for_continue()
        else:
            # Processar seleção por números
            new_selection = _parse_table_selection(choice, tables)
            if new_selection is not None:
                selected.update(new_selection)
                show_success_message(f"✅ {len(new_selection)} tabelas adicionadas à seleção!")


def _parse_table_selection(input_str: str, tables: List[TableInfo]) -> Optional[List[str]]:
    """
    Processa entrada de seleção de tabelas (ex: "1,3,5-8")
    
    Args:
        input_str: String de entrada
        tables: Lista de tabelas
        
    Returns:
        Optional[List[str]]: Lista de nomes de tabelas ou None se inválido
    """
    try:
        selected_names = []
        parts = input_str.split(',')
        
        for part in parts:
            part = part.strip()
            
            if '-' in part:
                # Range (ex: "5-8")
                start_str, end_str = part.split('-', 1)
                start = int(start_str.strip())
                end = int(end_str.strip())
                
                if start < 1 or end > len(tables) or start > end:
                    console.print(f"[red]❌ Range inválido: {part}[/red]")
                    return None
                
                for i in range(start, end + 1):
                    selected_names.append(tables[i - 1].name)
            else:
                # Número único
                index = int(part)
                if index < 1 or index > len(tables):
                    console.print(f"[red]❌ Número inválido: {index}[/red]")
                    return None
                
                selected_names.append(tables[index - 1].name)
        
        return selected_names
        
    except ValueError:
        console.print(f"[red]❌ Formato inválido: {input_str}[/red]")
        return None


def _show_current_selection(selected: set) -> None:
    """
    Mostra a seleção atual de tabelas
    
    Args:
        selected: Set de tabelas selecionadas
    """
    if not selected:
        console.print("[yellow]📭 Nenhuma tabela selecionada.[/yellow]")
        return
    
    console.print(f"\n[bold]✅ Tabelas Selecionadas ({len(selected)}):[/bold]")
    for i, table_name in enumerate(sorted(selected), 1):
        console.print(f"  {i:2d}. {table_name}")


def _wait_for_continue() -> None:
    """Aguarda o usuário pressionar Enter para continuar"""
    try:
        Prompt.ask("\n[dim]Pressione Enter para continuar...[/dim]", default="")
    except KeyboardInterrupt:
        pass


def _generate_jsonl_models_for_datasource(datasource: DataSource, selected_tables: List[str]) -> None:
    """
    Gera arquivos de modelo JSONL para as tabelas selecionadas de uma fonte de dados
    
    Args:
        datasource: Fonte de dados
        selected_tables: Lista de nomes das tabelas selecionadas
    """
    try:
        console.print(f"\n[yellow]📄 Gerando modelos JSONL para {len(selected_tables)} tabelas...[/yellow]")
        
        # Criar diretório de modelos
        models_dir = os.path.join(get_bridge_config_dir(), "models", datasource.name)
        os.makedirs(models_dir, exist_ok=True)
        
        # Converter DataSource para dict para compatibilidade
        datasource_dict = {
            'id': datasource.id,
            'name': datasource.name,
            'type': datasource.type,
            'connection': {
                'type': datasource.type,
                'host': datasource.conn.host,
                'port': datasource.conn.port,
                'database': datasource.conn.database,
                'user': datasource.conn.user,
                'password': datasource.conn.password,
                'options': datasource.conn.options
            }
        }
        
        # Conectar ao banco
        db_connector = create_database_connector(datasource_dict)
        
        generated_count = 0
        for table in selected_tables:
            try:
                console.print(f"  📄 Gerando modelo para tabela: [cyan]{table}[/cyan]")
                
                # Caminho do arquivo JSONL
                jsonl_file = os.path.join(models_dir, f"{table}.jsonl")
                
                # Obter estrutura das colunas
                columns = db_connector.get_table_columns(table)
                if not columns:
                    show_error_message(f"❌ Não foi possível obter estrutura da tabela {table}")
                    continue
                
                # Obter dados de amostra (sem pk_column pois não temos mapeamento ainda)
                sample_data = db_connector.sample_table_data(table, limit=100)
                
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
                        
                        console.print(f"    ✅ Modelo gerado com {len(sample_data)} registros reais")
                    else:
                        # Tabela vazia - criar 100 linhas em branco
                        for i in range(100):
                            empty_record = {}
                            for column in columns:
                                empty_record[column['name']] = ""
                            f.write(json.dumps(empty_record, ensure_ascii=False) + '\n')
                        
                        console.print(f"    ✅ Modelo gerado com 100 linhas em branco (tabela vazia)")
                
                generated_count += 1
                
            except Exception as e:
                logger.error(f"Erro ao gerar modelo JSONL para tabela {table}: {e}")
                show_error_message(f"❌ Erro ao gerar modelo para tabela {table}: {e}")
        
        if generated_count > 0:
            show_success_message(f"✅ {generated_count} modelos JSONL gerados em: {models_dir}")
        else:
            show_error_message("❌ Nenhum modelo JSONL foi gerado")
        
    except Exception as e:
        logger.exception(f"❌ Erro ao gerar modelos JSONL: {e}")
        show_error_message(f"Erro ao gerar modelos JSONL: {e}")
