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

---

## 🔒 Segurança

- Nenhum segredo é commitado. Use `.env` (git‑ignored) ou **store seguro** (ex.: OCI Vault, Secrets Manager).  
- Logs não imprimem segredos.

---

## 📜 Licença

MIT © DataSnap

---

## 🧭 Suporte

- Dúvidas de onboarding/DataSnap: entre em contato com o time DataSnap.
- Problemas no Bridge: abra uma issue com logs (`BRIDGE_LOG_LEVEL=DEBUG`).

