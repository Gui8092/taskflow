# Changelog

Todas as mudanças relevantes deste projeto são registradas aqui.
O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/) e o
versionamento segue [SemVer](https://semver.org/lang/pt-BR/).

## [Não publicado]

### Adicionado

- **Cancelamento de task:** novo estado terminal `CANCELLED`, alcançável a partir de
  `PENDING` e `RETRY` via `Broker.cancel(id, reason=...)`, com evento `cancelled`
  no bus, contagem por fila e badge próprio na interface. Cancelamento é decisão,
  não falha: a task não entra na dead letter queue e pode voltar com `requeue`.
  Task em `RUNNING` não pode ser cancelada e o erro explica que não há
  cancelamento cooperativo.
- **Métricas no formato Prometheus:** novo `taskflow/core/metrics.py`, exposto em
  `GET /metrics` (content type `text/plain; version=0.0.4`) e no comando
  `taskflow.cli metrics`, que lê o broker em modo somente-leitura (sem tomar a
  trava de escrita). Séries: `taskflow_tasks_total{queue,state}`, `taskflow_ready`,
  `taskflow_queued`, `taskflow_in_flight`, `taskflow_dead_lettered`,
  `taskflow_succeeded`, `taskflow_cancelled`, `taskflow_uptime_seconds` e
  `taskflow_journal_lines`.
- **Agendamento por intervalo:** `Schedule` agora aceita `cron` **ou** `interval`,
  e `Scheduler.add_interval(task, segundos, ...)` dispara abaixo da granularidade
  de 1 minuto do cron, sem acumular atraso quando o processo fica parado.
- **Novos comandos de CLI:** `cancel`, `metrics` e `scheduler` (roda o agendador
  enfileirando tasks, com `--workers N` para consumir no mesmo processo).
- **Novo endpoint `POST /api/tasks/{id}/cancel`** para a interface web.
- Testes: `tests/test_features.py` (19 casos) e 7 casos novos em `test_cli.py`.
  Total de 210 testes, todos passando.

### Documentado

- Limitações agora explícitas no README: escrita do journal em I/O bloqueante no
  event loop, ausência de encadeamento de tasks (`group`/`chord`), cancelamento
  apenas antes da execução e ausência de teste de integração HTTP/WebSocket
  (bloqueada pela restrição de dependências: `httpx` está fora da allowlist).

## [1.0.0] — 2026-10-02

Primeira versão estável: fila de tarefas distribuída completa, com broker,
persistência em disco, workers, retry, dead letter queue, agendador cron, CLI e
dashboard web — tudo implementado do zero, sem Celery, Redis, RQ ou Dramatiq.

### Broker e persistência

- Broker em memória com múltiplas filas e prioridade por heap `(-prioridade, sequência)`.
- Persistência em journal JSONL *append-only* (uma linha por mudança, com a task
  inteira) e snapshot atômico com compaction automática.
- Recuperação tolerante a falha: snapshot + replay, ignorando linha truncada por
  crash no meio da escrita.
- Leitura por outro processo via `refresh()`, para acompanhar tasks enfileiradas
  por fora sem tomar a trava de escrita.
- Trava de escrita exclusiva por diretório (`msvcrt` no Windows, `fcntl` no POSIX),
  com `--no-lock` / `TASKFLOW_LOCK=0` para escrita concorrente *best effort*.
- Limpeza do ledger acima de `TASKFLOW_LEDGER_LIMIT`, `fsync` opcional.

### Execução e resiliência

- Pool de workers assíncronos: N corrotinas, uma task por vez, sem *prefetch*.
- Leases com *fencing token*: `ack`/`nack` só valem com o `lease_id` corrente,
  então um worker lento não grava estado sobre a task que outro já reexecutou.
- Recuperação de task abandonada: lease vencida é recolhida pelo varredor; no
  *start* do processo, tasks em `RUNNING` voltam para `PENDENTE`.
- Heartbeat que renova a lease de tasks longas, evitando falso recolhimento.
- Timeout por task com `asyncio.timeout`, contado como falha normal.

### Retry e dead letter queue

- Backoff exponencial com *full jitter*, configurável, com `rng` injetável.
- `max_retries` conta retries (total de execuções = retries + 1).
- DLQ com listagem, filtro por fila, requeue (com orçamento novo de tentativas),
  requeue em lote e purge.
- Distinção entre `FAILED` (falha permanente: task não registrada, retorno não
  serializável) e `DEAD` (retries esgotados) — ambos na DLQ, com causas distintas.

### Agendador cron

- Parser de expressões cron de 5 campos sem dependências externas: `*`, `*/n`,
  `a`, `a-b`, `a-b/n`, listas, nomes de mês e dia da semana, `7` = domingo.
- Semântica do Vixie para dia do mês + dia da semana (OU entre os dois).
- `matches`, `next_after` com rollover de dia/mês/ano e erro explícito para
  expressões impossíveis (ex.: `0 0 30 2 *`).
- Loop com `clock`/`sleep` injetáveis e **sem tempestade de catch-up**: processo
  parado por 10 minutos dispara uma vez, não dez.

### CLI e dashboard

- `python -m taskflow.cli` com `submit`, `status`, `tasks`, `monitor`, `worker`,
  `dashboard`, `dlq` e `cron`.
- `monitor`: TUI ao vivo com redesenho ANSI, Tail de eventos e modo `--once`
  determinístico para pipes e testes.
- `dashboard`: FastAPI + WebSocket com página única (CSS e JS embutidos, sem CDN
  e sem build), abas Tasks / Dead letter / Eventos, filtros, busca, dark mode
  automático, responsivo e com `prefers-reduced-motion`.
- Botão *reenfileirar* na aba Dead letter, via `POST /api/dlq/{id}/requeue`.

### Qualidade

- 184 testes automatizados cobrindo broker, worker, retry/DLQ, cron, CLI,
  persistência e a infraestrutura do núcleo (configuração, estados, event bus),
  todos passando.
- Logging estruturado com campos extras em formato `chave=valor`.
- Type hints e docstrings em português em todas as funções e classes públicas.
- Integração contínua no GitHub Actions: suíte em Python 3.11/3.12/3.13, checagem
  de sintaxe, bloqueio de imports proibidos e um job *smoke* que exercita a CLI de
  ponta a ponta em runner limpo.

### Correções

- **Configuração por variável de ambiente quebrada para valores não textuais:**
  os conversores de `TASKFLOW_WORKER_CONCURRENCY`, `TASKFLOW_LEASE_SECONDS`,
  `TASKFLOW_RETRY_BASE`, `TASKFLOW_QUEUES`, `TASKFLOW_DASHBOARD_PORT` e demais
  campos numéricos/booleanos/listas recebiam a assinatura errada e levantavam
  `TypeError`. Todos os conversores agora seguem a assinatura única
  `(texto, nome_do_campo)` e cada variável é coberta por teste.
- **`data_dir` em branco virava o diretório atual:** `Config(data_dir="   ")`
  produzia `Path("")` → `.` e era aceito. Agora é recusado com `ConfigError`.

[1.0.0]: https://github.com/Gui8092/taskflow/releases/tag/v1.0.0