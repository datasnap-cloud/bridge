"""
Módulo responsavel por coletar telemetria do sistema e construir payloads padronizados.
"""

import os
import platform
import socket
import uuid
from datetime import datetime
from typing import Optional, Dict, Any

from core.secrets_store import secrets_store


class TelemetryCollector:
    """Coletor de telemetria do sistema e construtor de payloads"""
    
    def __init__(self):
        self.hostname = self._get_hostname()
        self.os_info = self._get_os_info()
        self.bridge_version = self._get_git_version() or "0.1.0"
        self._session_run_id = f"run-{uuid.uuid4().hex[:8]}"

    def _get_git_version(self) -> Optional[str]:
        try:
            import subprocess
            return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'], 
                                        stderr=subprocess.DEVNULL).decode('ascii').strip()
        except:
            return None


    def _get_hostname(self) -> str:
        try:
            return socket.gethostname()
        except:
            return "n/a"

    def _get_os_info(self) -> str:
        try:
            return f"{platform.system()} {platform.release()}"
        except:
            return "n/a"
            
    def _get_bridge_name(self) -> str:
        """Tenta obter o bridge_name da primeira API Key configurada"""
        try:
            keys = secrets_store.list_keys()
            if keys:
                # Retorna o primeiro bridge_name encontrado ou 'default'
                for key in keys:
                    if key.bridge_name:
                        return key.bridge_name
            return "default-bridge"
        except:
            return "unknown-bridge"

    def build_payload(
        self,
        event_type: str,
        status: str,
        source: Optional[str] = None,
        destination: Optional[str] = None,
        duration_ms: int = 0,
        items_processed: int = 0,
        bytes_uploaded: int = 0,
        retry_count: int = 0,
        error_message: Optional[str] = None,
        error_code: Optional[str] = None,
        error_stack: Optional[str] = None,
        error_context: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Constrói o payload padrão para envio de telemetria.
        """
        
        # Gerar idempotency key único para este evento
        idempotency_key = f"evt-{uuid.uuid4()}"
        
        return {
            "bridge_name": self._get_bridge_name(),
            "event_type": event_type,
            "status": status,
            "bridge_version": self.bridge_version,
            "sent_at": datetime.utcnow().isoformat() + "Z",
            "idempotency_key": idempotency_key,
            "host_hostname": self.hostname,
            "host_os": self.os_info,
            "run_id": self._session_run_id, # ID único da sessão do processo atual
            "duration_ms": duration_ms,
            "items_processed": items_processed,
            "bytes_uploaded": bytes_uploaded,
            "retry_count": retry_count,
            "source": source or "bridge-system",
            "destination": destination or "datasnap-cloud",
            "error_message": error_message,
            "error_code": error_code,
            "error_stack": error_stack,
            "error_context": error_context
        }

# Instância global
telemetry = TelemetryCollector()
