"""
Módulo de criptografia para o Bridge
Implementa AES-GCM com chave derivada do machine-id
"""

import os
import sys
import json
import hashlib
import platform
import subprocess
from typing import Optional, Tuple, Dict, Any
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
from cryptography.hazmat.backends import default_backend

from core.logger import logger


def get_machine_id() -> str:
    """
    Obtém um identificador único da máquina baseado no sistema operacional
    
    Returns:
        str: Machine ID único para a máquina
        
    Raises:
        RuntimeError: Se não conseguir obter o machine ID
    """
    system = platform.system().lower()
    
    try:
        if system == "windows":
            # Windows: HKLM\SOFTWARE\Microsoft\Cryptography\MachineGuid
            import winreg
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, 
                              r"SOFTWARE\Microsoft\Cryptography") as key:
                machine_id, _ = winreg.QueryValueEx(key, "MachineGuid")
                return machine_id.strip()
                
        elif system == "linux":
            # Linux: conteúdo de /etc/machine-id
            try:
                with open("/etc/machine-id", "r") as f:
                    return f.read().strip()
            except FileNotFoundError:
                # Fallback para /var/lib/dbus/machine-id
                with open("/var/lib/dbus/machine-id", "r") as f:
                    return f.read().strip()
                    
        elif system == "darwin":  # macOS
            # macOS: ioreg -rd1 -c IOPlatformExpertDevice | grep IOPlatformUUID
            result = subprocess.run([
                "ioreg", "-rd1", "-c", "IOPlatformExpertDevice"
            ], capture_output=True, text=True, check=True)
            
            for line in result.stdout.split('\n'):
                if 'IOPlatformUUID' in line:
                    # Extrair UUID da linha: "IOPlatformUUID" = "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
                    uuid = line.split('"')[3]
                    return uuid.strip()
            
            raise RuntimeError("IOPlatformUUID não encontrado")
            
        else:
            raise RuntimeError(f"Sistema operacional não suportado: {system}")
            
    except Exception as e:
        raise RuntimeError(f"Erro ao obter machine ID: {e}")


def derive_key(machine_id: str, info: str = "datasnap.bridge.v1") -> bytes:
    """
    Deriva uma chave de 32 bytes usando Scrypt com o machine_id como salt
    
    Args:
        machine_id: ID único da máquina
        info: Informação adicional para derivação
        
    Returns:
        bytes: Chave de 32 bytes
    """
    # Usar machine_id como salt (hash para garantir tamanho consistente)
    salt = hashlib.sha256(machine_id.encode()).digest()
    
    # Usar info como password
    password = info.encode()
    
    # Configurar Scrypt (parâmetros balanceados para segurança/performance)
    kdf = Scrypt(
        length=32,  # 32 bytes = 256 bits
        salt=salt,
        n=2**14,    # 16384 - fator de custo
        r=8,        # tamanho do bloco
        p=1,        # paralelismo
        backend=default_backend()
    )
    
    return kdf.derive(password)


def encrypt_data(data: Dict[str, Any], key: bytes) -> bytes:
    """
    Criptografa dados usando AES-GCM
    
    Args:
        data: Dados para criptografar (serão convertidos para JSON)
        key: Chave de criptografia (32 bytes)
        
    Returns:
        bytes: nonce (12 bytes) + ciphertext + tag concatenados
    """
    logger.debug("🔐 Iniciando criptografia de dados")
    # Converter dados para JSON
    json_data = json.dumps(data, ensure_ascii=False).encode('utf-8')
    logger.debug(f"📄 Dados convertidos para JSON: {len(json_data)} bytes")
    
    # Gerar nonce aleatório (12 bytes para GCM)
    nonce = os.urandom(12)
    logger.debug("🎲 Nonce aleatório gerado")
    
    # Criar cipher AES-GCM
    aesgcm = AESGCM(key)
    
    # Criptografar (retorna ciphertext + tag concatenados)
    ciphertext_with_tag = aesgcm.encrypt(nonce, json_data, None)
    logger.debug(f"🔒 Dados criptografados: {len(ciphertext_with_tag)} bytes")
    
    # Concatenar nonce + ciphertext + tag
    result = nonce + ciphertext_with_tag
    logger.debug(f"✅ Criptografia concluída: {len(result)} bytes totais")
    return result


def decrypt_data(encrypted_data: bytes, key: bytes) -> Dict[str, Any]:
    """
    Descriptografa dados usando AES-GCM
    
    Args:
        encrypted_data: Dados criptografados (nonce + ciphertext + tag)
        key: Chave de descriptografia (32 bytes)
        
    Returns:
        Dict[str, Any]: Dados descriptografados
        
    Raises:
        ValueError: Se a descriptografia falhar
    """
    logger.debug(f"🔓 Iniciando descriptografia de {len(encrypted_data)} bytes")
    if len(encrypted_data) < 12:
        logger.error("❌ Dados criptografados muito pequenos")
        raise ValueError("Dados criptografados muito pequenos")
    
    # Extrair nonce (primeiros 12 bytes)
    nonce = encrypted_data[:12]
    logger.debug("🎲 Nonce extraído dos dados")
    
    # Extrair ciphertext + tag (resto dos bytes)
    ciphertext_with_tag = encrypted_data[12:]
    logger.debug(f"📄 Ciphertext extraído: {len(ciphertext_with_tag)} bytes")
    
    # Criar cipher AES-GCM
    aesgcm = AESGCM(key)
    
    try:
        # Descriptografar
        json_data = aesgcm.decrypt(nonce, ciphertext_with_tag, None)
        logger.debug(f"🔓 Dados descriptografados: {len(json_data)} bytes")
        
        # Converter de JSON para dict
        result = json.loads(json_data.decode('utf-8'))
        logger.debug("✅ Descriptografia e parsing JSON concluídos")
        return result
        
    except Exception as e:
        logger.exception(f"❌ Falha na descriptografia: {e}")
        raise ValueError(f"Falha na descriptografia: {e}")


def encrypt_data_to_file(data: Dict[str, Any], file_path: str) -> None:
    """
    Criptografa dados e salva em arquivo
    
    Args:
        data: Dados para criptografar (serão convertidos para JSON)
        file_path: Caminho do arquivo onde salvar os dados criptografados
        
    Raises:
        Exception: Se houver erro na criptografia ou salvamento
    """
    logger.debug(f"💾 Salvando dados criptografados em {file_path}")
    try:
        # Obter chave de criptografia
        key = get_encryption_key()
        
        # Criptografar dados
        encrypted_data = encrypt_data(data, key)
        
        # Garantir que o diretório pai existe
        from pathlib import Path
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Salvar arquivo
        with open(file_path, 'wb') as f:
            f.write(encrypted_data)
        
        logger.debug(f"✅ Dados salvos com sucesso em {file_path}")
        
    except Exception as e:
        logger.exception(f"❌ Erro ao salvar dados criptografados: {e}")
        raise Exception(f"Erro ao salvar dados criptografados: {e}")


def decrypt_data_from_file(file_path: str) -> Dict[str, Any]:
    """
    Carrega e descriptografa dados de arquivo
    
    Args:
        file_path: Caminho do arquivo com dados criptografados
        
    Returns:
        Dict[str, Any]: Dados descriptografados
        
    Raises:
        FileNotFoundError: Se o arquivo não existir
        Exception: Se houver erro na descriptografia
    """
    logger.debug(f"📂 Carregando dados criptografados de {file_path}")
    try:
        # Verificar se arquivo existe
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        
        # Ler arquivo
        with open(file_path, 'rb') as f:
            encrypted_data = f.read()
        
        # Obter chave de descriptografia
        key = get_encryption_key()
        
        # Descriptografar dados
        decrypted_data = decrypt_data(encrypted_data, key)
        
        logger.debug(f"✅ Dados carregados com sucesso de {file_path}")
        return decrypted_data
        
    except Exception as e:
        logger.exception(f"❌ Erro ao carregar dados criptografados: {e}")
        raise Exception(f"Erro ao carregar dados criptografados: {e}")


def get_encryption_key() -> bytes:
    """
    Obtém a chave de criptografia derivada do machine-id
    
    Returns:
        bytes: Chave de criptografia (32 bytes)
        
    Raises:
        RuntimeError: Se não conseguir obter o machine ID
    """
    machine_id = get_machine_id()
    return derive_key(machine_id)