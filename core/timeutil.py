"""
Utilitários para manipulação de tempo e timestamps.

Este módulo fornece funções para trabalhar com timestamps,
formatação de datas e cálculos de tempo para o pipeline de sincronização.
"""

import time
from datetime import datetime, timezone
from typing import Optional, Union


def get_current_timestamp() -> int:
    """
    Retorna o timestamp atual em segundos desde epoch.
    
    Returns:
        int: Timestamp atual em segundos
    """
    return int(time.time())


def get_current_timestamp_ms() -> int:
    """
    Retorna o timestamp atual em milissegundos desde epoch.
    
    Returns:
        int: Timestamp atual em milissegundos
    """
    return int(time.time() * 1000)


def timestamp_to_iso(timestamp: Union[int, float]) -> str:
    """
    Converte um timestamp para formato ISO 8601.
    
    Args:
        timestamp: Timestamp em segundos ou milissegundos
        
    Returns:
        str: Data no formato ISO 8601
    """
    # Se o timestamp parece estar em milissegundos, converte para segundos
    if timestamp > 1e10:
        timestamp = timestamp / 1000
    
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.isoformat()


def iso_to_timestamp(iso_string: str) -> int:
    """
    Converte uma string ISO 8601 para timestamp.
    
    Args:
        iso_string: Data no formato ISO 8601
        
    Returns:
        int: Timestamp em segundos
    """
    dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
    return int(dt.timestamp())


def format_duration(seconds: Union[int, float]) -> str:
    """
    Formata uma duração em segundos para formato legível.
    
    Args:
        seconds: Duração em segundos
        
    Returns:
        str: Duração formatada (ex: "2m 30s", "1h 15m 30s")
    """
    seconds = int(seconds)
    
    if seconds < 60:
        return f"{seconds}s"
    
    minutes = seconds // 60
    remaining_seconds = seconds % 60
    
    if minutes < 60:
        if remaining_seconds > 0:
            return f"{minutes}m {remaining_seconds}s"
        return f"{minutes}m"
    
    hours = minutes // 60
    remaining_minutes = minutes % 60
    
    parts = [f"{hours}h"]
    if remaining_minutes > 0:
        parts.append(f"{remaining_minutes}m")
    if remaining_seconds > 0:
        parts.append(f"{remaining_seconds}s")
    
    return " ".join(parts)


def get_time_window(window_minutes: int = 5) -> tuple[int, int]:
    """
    Retorna uma janela de tempo baseada no timestamp atual.
    
    Args:
        window_minutes: Tamanho da janela em minutos (padrão: 5)
        
    Returns:
        tuple: (timestamp_inicio, timestamp_fim) em segundos
    """
    now = get_current_timestamp()
    window_seconds = window_minutes * 60
    
    # Alinha o início da janela com múltiplos do tamanho da janela
    start_time = (now // window_seconds) * window_seconds
    end_time = start_time + window_seconds
    
    return start_time, end_time


def sleep_until_next_window(window_minutes: int = 5) -> None:
    """
    Dorme até o início da próxima janela de tempo.
    
    Args:
        window_minutes: Tamanho da janela em minutos (padrão: 5)
    """
    _, next_window_start = get_time_window(window_minutes)
    current_time = get_current_timestamp()
    
    if next_window_start > current_time:
        sleep_time = next_window_start - current_time
        print(f"Aguardando {format_duration(sleep_time)} até a próxima janela...")
        time.sleep(sleep_time)


def is_timestamp_recent(timestamp: Union[int, float], max_age_minutes: int = 60) -> bool:
    """
    Verifica se um timestamp é recente (dentro do limite especificado).
    
    Args:
        timestamp: Timestamp a verificar
        max_age_minutes: Idade máxima em minutos (padrão: 60)
        
    Returns:
        bool: True se o timestamp é recente
    """
    # Se o timestamp parece estar em milissegundos, converte para segundos
    if timestamp > 1e10:
        timestamp = timestamp / 1000
    
    current_time = get_current_timestamp()
    max_age_seconds = max_age_minutes * 60
    
    return (current_time - timestamp) <= max_age_seconds


class Timer:
    """
    Classe para medir tempo de execução de operações.
    """
    
    def __init__(self):
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
    
    def start(self) -> None:
        """Inicia o timer."""
        self.start_time = time.time()
        self.end_time = None
    
    def stop(self) -> float:
        """
        Para o timer e retorna o tempo decorrido.
        
        Returns:
            float: Tempo decorrido em segundos
        """
        if self.start_time is None:
            raise ValueError("Timer não foi iniciado")
        
        self.end_time = time.time()
        return self.end_time - self.start_time
    
    def elapsed(self) -> float:
        """
        Retorna o tempo decorrido sem parar o timer.
        
        Returns:
            float: Tempo decorrido em segundos
        """
        if self.start_time is None:
            raise ValueError("Timer não foi iniciado")
        
        current_time = time.time()
        return current_time - self.start_time
    
    def __enter__(self):
        """Suporte para context manager."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Suporte para context manager."""
        self.stop()