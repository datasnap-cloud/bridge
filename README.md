# Bridge (DataSnap)

**Bridge** é o utilitário **oficial** da DataSnap para **migração e transferência automática de dados**.  
Feito para squads que querem começar **em minutos**: CLI pronta, templates de configuração e conectores para
**filesystem local**, **S3/compatível** e **OCI Object Storage** (com suporte incremental no roadmap).

---

## ✨ Por que usar o Bridge?

- **Comece rápido**: gere um `bridge.yaml` de exemplo e rode migrações em minutos.
- **Pense em fluxo, não em plumbing**: CLI simples, logs legíveis e *dry‑run*.
- **Pronto para DataSnap**: caminhos, convenções e validações pensadas para ingestão na plataforma.

---

## 🚀 Instalação (dev)

```bash
a fazer
```

Requisitos: Python ≥ 3.9.

---

## 🏁 Começo rápido

```bash
a fazer
```

---

## 🔧 Configuração

```bash
a fazer
```

---

## 📋 Comandos Disponíveis

O Bridge oferece os seguintes comandos CLI:

### `bridge setup`
Menu interativo para configurar API Keys, Fontes de Dados e consultar Schemas da DataSnap.

Este comando abre um menu TUI (Terminal User Interface) que permite:
- **Cadastrar e validar API Keys** da DataSnap
- **Listar API Keys cadastradas** (com tokens mascarados para segurança)
- **Gerenciar Fontes de Dados** (MySQL e PostgreSQL)
  - Criar novas conexões de banco de dados
  - Validar conectividade antes de salvar
  - Listar fontes cadastradas
  - Cadastrar tabelas de uma fonte específica
  - Excluir fontes de dados
- **Consultar Modelos de Dados (Schemas)** da API
- **Gerenciar configurações** de forma segura

Todos os dados sensíveis são criptografados com AES-GCM e armazenados localmente.

```bash
python -m bridge setup
```

### `bridge status`
Exibe o status do sistema e conectividade com a API DataSnap.

Mostra informações sobre:
- **Número de API Keys cadastradas**
- **Status da conectividade** com a API DataSnap
- **Informações do sistema**

```bash
python -m bridge status
```

### `bridge version`
Exibe informações sobre a versão atual do Bridge.

```bash
python -m bridge version
```

---

## 🧪 Exemplos de uso

### Local → Local (MVP)
```bash
a fazer
```

## 🧠 Dicas de performance (gerais)

- Prefira **arquivos de até 10MB** para melhor performance de ingestão de dados.
- Evite milhões de pequenos arquivos.

---

## 🤝 Contribuindo

- Abra issues com **casos reais de migração** (tamanho, origem/destino, volume de arquivos).
- Pull requests bem-vindos — mantenha estilo dos módulos existentes e cobertura básica de testes.

---

## ❓ FAQ

**Bridge reprocessa arquivos já migrados?**  
Por padrão, vamos incluir **checks de idempotência** no roadmap (hash/etag/size).

**Preciso do PyArrow para começar?**  
Não. Parquet e row‑count são opcionais (apenas se você quiser validações/transformações).

**Quais bancos de dados são suportados?**  
Atualmente: **MySQL** e **PostgreSQL**. Mais conectores no roadmap.

**Como funciona a validação de conexão?**  
O Bridge testa a conectividade executando `SELECT 1` antes de salvar as credenciais.

**Posso selecionar tabelas específicas?**  
Sim! Após cadastrar uma fonte, use "Cadastrar tabelas" para descobrir e selecionar tabelas específicas.

---

## 🔒 Segurança

- Nenhum segredo é commitado. Use `.env` (git‑ignored) ou **store seguro** (ex.: OCI Vault, Secrets Manager).  
- Logs não imprimem segredos.
- **Criptografia forte**: Todos os dados sensíveis (API Keys, credenciais de banco) são criptografados com **AES-GCM**.
- **Chaves derivadas**: Utiliza **Argon2id** para derivação de chaves baseada no `machine-id` do sistema.
- **Permissões restritivas**: Arquivos `.enc` são criados com permissões `0o600` (apenas proprietário).
- **Nonces únicos**: Cada operação de criptografia utiliza um nonce aleatório de 12 bytes.

### Arquivos de Configuração

O Bridge armazena dados na pasta `.bridge/` ao lado do executável:

- `.bridge/config.json` - Configurações gerais (não criptografado)
- `.bridge/api_keys.enc` - API Keys da DataSnap (criptografado)
- `.bridge/datasources.enc` - Credenciais de fontes de dados (criptografado)

---

## 📜 Licença

MIT © DataSnap

---

## 🧭 Suporte

- Dúvidas de onboarding/DataSnap: entre em contato com o time DataSnap.
- Problemas no Bridge: abra uma issue com logs (`BRIDGE_LOG_LEVEL=DEBUG`).

