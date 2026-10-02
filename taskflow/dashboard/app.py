"""Dashboard web do taskflow: FastAPI + WebSocket + página única.

Endpoints
---------
``GET /``
    página HTML única (CSS e JS embutidos, sem CDN e sem build)
``GET /api/state``
    fotografia completa (stats, tasks, DLQ, eventos) em JSON
``GET /api/tasks``
    tasks filtradas por estado/fila
``GET /api/dlq``
    tasks da dead letter queue
``POST /api/dlq/{task_id}/requeue``
    reenfileira uma task da DLQ
``GET /health``
    verificação rápida
``WS /ws``
    envia um snapshot ao conectar e a cada evento, com coalescência (~4 msg/s)

O dashboard é **observador por padrão**: ele abre o broker e apenas observa, sem
subir workers. Para uma demonstração autocontida, use ``--workers N`` (ou
``TASKFLOW_DASHBOARD_WORKERS=N``) e o worker embutido aparece na mesma página.
Se outro processo já estiver escrevendo no ``data_dir``, o dashboard cai para
modo somente-leitura e o botão de requeue é desabilitado.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Final

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from taskflow.core.broker import (
    Broker,
    BrokerError,
    BrokerLockedError,
    ReadOnlyBrokerError,
    TaskNotFoundError,
)
from taskflow.core.config import Config
from taskflow.core.registry import TaskRegistry
from taskflow.core.states import TaskState
from taskflow.core.task import Task, TaskFilter
from taskflow.worker.deadletter import DeadLetterQueue
from taskflow.worker.pool import WorkerPool

LOGGER: Final[logging.Logger] = logging.getLogger("taskflow.dashboard")

#: Intervalo mínimo entre dois snapshots enviados pelo WebSocket.
PUSH_INTERVAL: Final[float] = 0.25

#: Janela de coalescência: agrupa eventos que chegam juntos.
COALESCE_WINDOW: Final[float] = 0.05

#: Intervalo do keepalive quando nada acontece.
KEEPALIVE_INTERVAL: Final[float] = 15.0

#: Intervalo com que o dashboard relê o journal para enxergar escritas externas.
JOURNAL_POLL: Final[float] = 0.4

#: Quantidade máxima de tasks enviadas no snapshot.
SNAPSHOT_TASKS: Final[int] = 60

#: Quantidade máxima de eventos enviados no snapshot.
SNAPSHOT_EVENTS: Final[int] = 40


async def _acompanhar_journal(broker: Broker) -> None:
    """Relê o journal periodicamente, para refletir tasks enfileiradas por outro processo."""
    while True:
        await asyncio.sleep(JOURNAL_POLL)
        try:
            if broker.refresh():
                LOGGER.debug("journal relido", extra={"registros": broker.journal_lines})
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("falha ao reler o journal")


def build_state_snapshot(
    broker: Broker,
    *,
    task_limit: int = SNAPSHOT_TASKS,
    dlq_limit: int = 30,
    event_limit: int = SNAPSHOT_EVENTS,
    worker_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Monta a fotografia completa do broker para a interface.

    Args:
        broker: Broker a inspecionar.
        task_limit: Quantidade de tasks recentes no snapshot.
        dlq_limit: Quantidade de tasks da DLQ no snapshot.
        event_limit: Quantidade de eventos recentes.
        worker_stats: Estatísticas do worker embutido, quando existir.

    Returns:
        Dicionário JSON-safe com ``stats``, ``tasks``, ``dlq``, ``events`` e ``queues``.
    """
    stats = broker.stats()
    tasks = broker.list_tasks(TaskFilter(limit=task_limit))
    dead = DeadLetterQueue(broker).list(limit=dlq_limit)
    durations = [
        task.duration_ms
        for task in broker.list_tasks(TaskFilter(limit=0))
        if task.state is TaskState.SUCCESS and task.duration_ms is not None
    ]
    return {
        "generated_at": time.time(),
        "read_only": broker.read_only,
        "stats": {
            **stats.to_dict(),
            "avg_duration_ms": round(sum(durations) / len(durations), 1) if durations else None,
        },
        "tasks": [task.summary() for task in tasks],
        "dlq": [task.summary() for task in dead],
        "events": [event.to_dict() for event in broker.events.history(event_limit)],
        "worker_stats": worker_stats,
    }


def build_task_payload(
    broker: Broker,
    *,
    state: str | None = None,
    queue: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Devolve as tasks em JSON, já filtradas."""
    parsed = TaskState.coerce(state) if state else None
    return [task.summary() for task in broker.list_tasks(TaskFilter(state=parsed, queue=queue, limit=limit))]


def build_dlq_payload(broker: Broker, *, limit: int = 50) -> list[dict[str, Any]]:
    """Devolve as tasks da DLQ em JSON."""
    return [task.summary() for task in DeadLetterQueue(broker).list(limit=limit)]


def build_dashboard_html() -> str:
    """Devolve o HTML completo do dashboard (string estática, sem dependências)."""
    return _DASHBOARD_HTML


def create_app(
    config: Config | None = None,
    registry: TaskRegistry | None = None,
    *,
    start_workers: int | None = None,
    broker: Broker | None = None,
    write: bool = True,
) -> FastAPI:
    """Cria a aplicação FastAPI do dashboard.

    Args:
        config: Configuração (padrão: a do processo).
        registry: Registro de tasks usado quando ``start_workers`` é informado.
        start_workers: Quantidade de workers embutidos (0 ou ``None`` = apenas observar).
        broker: Broker já pronto (usado em testes); um novo é criado no lifespan.
        write: Tentar abrir o broker com escrita; se outro processo estiver usando o
            ``data_dir``, cai para somente-leitura em vez de falhar.

    Returns:
        A aplicação configurada, pronta para o ``uvicorn.run``.
    """
    settings = config if config is not None else Config.from_env()
    tasks_registry = registry if registry is not None else TaskRegistry()
    preloaded = broker

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        """Abre o broker (e o worker embutido, se houver) e fecha tudo ao encerrar."""
        active = preloaded
        if active is None:
            active = Broker(settings, tasks_registry, read_only=not write)
            try:
                await active.start()
            except BrokerLockedError as exc:
                if not write:
                    raise
                LOGGER.warning("dashboard em modo somente-leitura: %s", exc)
                active = Broker(settings, tasks_registry, read_only=True)
                await active.start()
        pool: WorkerPool | None = None
        if start_workers:
            pool = WorkerPool(
                active,
                tasks_registry,
                settings,
                concurrency=start_workers,
                queues=tuple(settings.queues),
                worker_prefix="dashboard",
            )
            await pool.start()
            LOGGER.info("workers embutidos iniciados", extra={"quantidade": start_workers})
        refresher = asyncio.create_task(_acompanhar_journal(active), name="taskflow:dashboard-refresh")
        application.state.broker = active
        application.state.pool = pool
        application.state.config = settings
        application.state.registry = tasks_registry
        try:
            yield
        finally:
            refresher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await refresher
            if pool is not None:
                await pool.stop(drain=False, timeout=5.0)
            if preloaded is None:
                await active.stop()

    application = FastAPI(
        title="taskflow",
        description="Dashboard de uma fila de tarefas distribuída implementada do zero",
        version="1.0.0",
        lifespan=lifespan,
    )

    def snapshot() -> dict[str, Any]:
        """Shortcut que junta o snapshot com as estatísticas do worker embutido."""
        active: Broker = application.state.broker
        pool: WorkerPool | None = getattr(application.state, "pool", None)
        return build_state_snapshot(active, worker_stats=pool.stats() if pool else None)

    @application.get("/", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        """Serve a página única do dashboard."""
        return HTMLResponse(build_dashboard_html())

    @application.get("/health")
    async def health() -> dict[str, Any]:
        """Verificação de saúde com o estado do broker."""
        active: Broker = application.state.broker
        stats = active.stats()
        return {
            "ok": True,
            "read_only": active.read_only,
            "total": stats.total,
            "ready": stats.ready,
            "in_flight": stats.in_flight,
        }

    @application.get("/api/state")
    async def api_state() -> JSONResponse:
        """Devolve a fotografia completa do broker."""
        return JSONResponse(snapshot())

    @application.get("/api/tasks")
    async def api_tasks(
        state: str | None = Query(default=None, description="filtra por estado"),
        queue: str | None = Query(default=None, description="filtra por fila"),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict[str, Any]:
        """Devolve as tasks, opcionalmente filtradas."""
        active: Broker = application.state.broker
        return {"tasks": build_task_payload(active, state=state, queue=queue, limit=limit)}

    @application.get("/api/dlq")
    async def api_dlq(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
        """Devolve as tasks da dead letter queue."""
        active: Broker = application.state.broker
        return {
            "tasks": build_dlq_payload(active, limit=limit),
            "reasons": DeadLetterQueue(active).reasons(),
            "read_only": active.read_only,
        }

    @application.post("/api/dlq/{task_id}/requeue")
    async def api_requeue(task_id: str, queue: str | None = None) -> JSONResponse:
        """Reenfileira uma task da DLQ (botão da interface)."""
        active: Broker = application.state.broker
        try:
            task: Task = await DeadLetterQueue(active).requeue(task_id, queue=queue)
        except ReadOnlyBrokerError as exc:
            return JSONResponse({"error": str(exc)}, status_code=409)
        except TaskNotFoundError as exc:
            return JSONResponse({"error": str(exc)}, status_code=404)
        except BrokerError as exc:
            return JSONResponse({"error": str(exc)}, status_code=409)
        return JSONResponse({"ok": True, "task": task.summary()})

    @application.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        """Envia snapshots pela conexão WebSocket, coalescendo rajadas de eventos."""
        await websocket.accept()
        active: Broker = application.state.broker
        pool: WorkerPool | None = getattr(application.state, "pool", None)
        wakeup: asyncio.Queue[bool] = asyncio.Queue(maxsize=1)

        def notify(event: Any) -> None:
            """Chamado pelo event bus a cada mudança; só marca que há novelty."""
            if not wakeup.full():
                wakeup.put_nowait(True)

        subscription = active.events.subscribe(notify, name="dashboard-ws")
        last_sent = 0.0
        try:
            await websocket.send_json({"type": "snapshot", "data": snapshot()})
            last_sent = time.monotonic()
            while True:
                try:
                    async with asyncio.timeout(KEEPALIVE_INTERVAL):
                        await wakeup.get()
                except TimeoutError:
                    await websocket.send_json({"type": "ping"})
                    continue
                await asyncio.sleep(COALESCE_WINDOW)
                while not wakeup.empty():
                    wakeup.get_nowait()
                elapsed = time.monotonic() - last_sent
                if elapsed < PUSH_INTERVAL:
                    await asyncio.sleep(PUSH_INTERVAL - elapsed)
                payload = (
                    build_state_snapshot(active, worker_stats=pool.stats() if pool else None)
                )
                await websocket.send_json({"type": "snapshot", "data": payload})
                last_sent = time.monotonic()
        except WebSocketDisconnect:
            LOGGER.debug("websocket desconectado")
        except Exception:
            LOGGER.exception("falha no websocket do dashboard")
        finally:
            subscription.unsubscribe()

    return application


_DASHBOARD_HTML: Final[str] = """<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>taskflow · dashboard</title>
<style>
:root {
  --bg: #ffffff;
  --bg-recessed: #f9fafb;
  --bg-raised: #ffffff;
  --border: #e5e7eb;
  --border-strong: #d1d5db;
  --text: #111827;
  --text-2: #4b5563;
  --text-3: #9ca3af;
  --accent: #111827;
  --ok: #047857; --ok-bg: #ecfdf5; --ok-border: #a7f3d0;
  --warn: #b45309; --warn-bg: #fffbeb; --warn-border: #fde68a;
  --bad: #b91c1c; --bad-bg: #fef2f2; --bad-border: #fecaca;
  --info: #6d28d9; --info-bg: #f5f3ff; --info-border: #ddd6fe;
  --muted: #4b5563; --muted-bg: #f3f4f6; --muted-border: #e5e7eb;
  --shadow: 0 1px 2px rgba(16, 24, 40, .05);
  --radius-sm: 6px;
  --radius-md: 10px;
  --radius-full: 999px;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0a0a0a;
    --bg-recessed: rgba(255, 255, 255, .04);
    --bg-raised: rgba(255, 255, 255, .07);
    --border: rgba(255, 255, 255, .12);
    --border-strong: rgba(255, 255, 255, .2);
    --text: #f3f4f6;
    --text-2: #d1d5db;
    --text-3: #6b7280;
    --accent: #ffffff;
    --ok: #34d399; --ok-bg: rgba(52, 211, 153, .12); --ok-border: rgba(52, 211, 153, .3);
    --warn: #fbbf24; --warn-bg: rgba(251, 191, 36, .12); --warn-border: rgba(251, 191, 36, .3);
    --bad: #f87171; --bad-bg: rgba(248, 113, 113, .12); --bad-border: rgba(248, 113, 113, .3);
    --info: #a78bfa; --info-bg: rgba(167, 139, 250, .12); --info-border: rgba(167, 139, 250, .3);
    --muted: #9ca3af; --muted-bg: rgba(255, 255, 255, .07); --muted-border: rgba(255, 255, 255, .12);
    --shadow: none;
  }
}
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; }
body {
  background: var(--bg);
  color: var(--text);
  font: 13px/1.5 ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
}
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; }
.shell { display: grid; grid-template-columns: 232px minmax(0, 1fr); min-height: 100vh; }
.rail {
  border-right: 1px solid var(--border);
  padding: 20px 16px;
  display: flex; flex-direction: column; gap: 22px;
  background: var(--bg-recessed);
}
.brand { display: flex; align-items: center; gap: 9px; }
.brand-dot { width: 9px; height: 9px; border-radius: var(--radius-full); background: var(--text-3); }
.brand-dot.live { background: var(--ok); animation: pulse 2s ease-in-out infinite; }
.brand-dot.off { background: var(--bad); }
.brand-name { font-size: 15px; font-weight: 650; letter-spacing: -.01em; }
.brand-sub { font-size: 11.5px; color: var(--text-3); }
.label {
  font-size: 11px; font-weight: 600; letter-spacing: .06em; text-transform: uppercase;
  color: var(--text-3); margin-bottom: 8px;
}
.queue-row {
  display: flex; align-items: center; justify-content: space-between; gap: 8px;
  padding: 6px 9px; border-radius: var(--radius-sm); font-size: 12.5px;
}
.queue-row.active { background: var(--bg-raised); border: 1px solid var(--border); }
.queue-row .count {
  font-size: 11.5px; color: var(--text-2); background: var(--muted-bg);
  border: 1px solid var(--muted-border); border-radius: var(--radius-full);
  padding: 0 7px; min-width: 22px; text-align: center;
}
.state-legend { display: flex; flex-direction: column; gap: 5px; }
.legend-row { display: flex; align-items: center; gap: 7px; font-size: 12.5px; color: var(--text-2); }
.legend-row b { margin-left: auto; font-weight: 600; color: var(--text); font-variant-numeric: tabular-nums; }
.rail-foot { margin-top: auto; font-size: 11px; color: var(--text-3); line-height: 1.6; }
.main { padding: 20px 24px 32px; display: flex; flex-direction: column; gap: 16px; min-width: 0; }
.head { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; flex-wrap: wrap; }
.head h1 { margin: 0; font-size: 19px; font-weight: 650; letter-spacing: -.015em; }
.head p { margin: 3px 0 0; color: var(--text-2); font-size: 12.5px; }
.pill {
  display: inline-flex; align-items: center; gap: 7px; padding: 4px 11px;
  border: 1px solid var(--border); border-radius: var(--radius-full);
  font-size: 12px; color: var(--text-2); background: var(--bg-raised); box-shadow: var(--shadow);
}
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; }
.tile { border: 1px solid var(--border); border-radius: var(--radius-md); padding: 12px 14px; background: var(--bg-raised); box-shadow: var(--shadow); }
.tile .k { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: var(--text-3); }
.tile .v { font-size: 25px; font-weight: 620; letter-spacing: -.02em; margin-top: 2px; font-variant-numeric: tabular-nums; }
.tile .s { font-size: 11.5px; color: var(--text-2); }
.panel { border: 1px solid var(--border); border-radius: var(--radius-md); background: var(--bg-raised); box-shadow: var(--shadow); overflow: hidden; }
.tabs { display: flex; gap: 2px; padding: 8px 10px 0; border-bottom: 1px solid var(--border); }
.tab {
  border: 0; background: transparent; color: var(--text-2); font: inherit; font-size: 12.5px;
  padding: 7px 12px; border-radius: var(--radius-sm) var(--radius-sm) 0 0; cursor: pointer;
  border-bottom: 2px solid transparent; margin-bottom: -1px;
}
.tab:hover { background: var(--bg-recessed); color: var(--text); }
.tab[aria-selected="true"] { color: var(--text); border-bottom-color: var(--accent); font-weight: 600; }
.tab:focus-visible, .btn:focus-visible, select:focus-visible, input:focus-visible {
  outline: 2px solid var(--accent); outline-offset: 2px;
}
.tab .badge { margin-left: 6px; }
.filters { display: flex; gap: 8px; align-items: center; padding: 10px 12px; border-bottom: 1px solid var(--border); flex-wrap: wrap; }
select, input[type="search"], .btn {
  font: inherit; font-size: 12.5px; color: var(--text); background: var(--bg);
  border: 1px solid var(--border-strong); border-radius: var(--radius-sm); padding: 5px 9px;
}
input[type="search"] { min-width: 190px; }
.btn { cursor: pointer; transition: background-color .15s ease; }
.btn:hover { background: var(--bg-recessed); }
.btn.primary { background: var(--accent); color: var(--bg); border-color: var(--accent); }
.btn.primary:hover { opacity: .88; }
.btn.small { padding: 3px 8px; font-size: 11.5px; }
.btn:disabled { cursor: not-allowed; opacity: .55; }
.spacer { flex: 1; }
table { width: 100%; border-collapse: collapse; font-size: 12.5px; }
thead th {
  text-align: left; font-size: 11px; font-weight: 600; letter-spacing: .05em; text-transform: uppercase;
  color: var(--text-3); padding: 9px 12px; border-bottom: 1px solid var(--border); white-space: nowrap;
}
tbody td { padding: 8px 12px; border-bottom: 1px solid var(--border); vertical-align: top; }
tbody tr:last-child td { border-bottom: 0; }
tbody tr { transition: background-color .15s ease; }
tbody tr:hover { background: var(--bg-recessed); }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
td.name { max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.badge {
  display: inline-flex; align-items: center; gap: 5px; padding: 2px 8px; border-radius: var(--radius-full);
  font-size: 11px; font-weight: 600; letter-spacing: .02em; border: 1px solid transparent; white-space: nowrap;
}
.badge.PENDING { color: var(--muted); background: var(--muted-bg); border-color: var(--muted-border); }
.badge.RUNNING { color: var(--info); background: var(--info-bg); border-color: var(--info-border); }
.badge.RETRY { color: var(--warn); background: var(--warn-bg); border-color: var(--warn-border); }
.badge.SUCCESS { color: var(--ok); background: var(--ok-bg); border-color: var(--ok-border); }
.badge.FAILED, .badge.DEAD { color: var(--bad); background: var(--bad-bg); border-color: var(--bad-border); }
.badge.RUNNING::before {
  content: ""; width: 5px; height: 5px; border-radius: 50%; background: currentColor;
  animation: pulse 1.3s ease-in-out infinite;
}
.err { color: var(--bad); }
.dim { color: var(--text-3); }
.events { max-height: 340px; overflow: auto; }
.event {
  display: flex; gap: 10px; align-items: baseline; padding: 7px 12px;
  border-bottom: 1px solid var(--border); font-size: 12.5px;
}
.event:last-child { border-bottom: 0; }
.event .t { color: var(--text-3); font-size: 11.5px; min-width: 58px; }
.event .tag { min-width: 74px; }
.event .msg { color: var(--text-2); min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.empty { padding: 34px 16px; text-align: center; color: var(--text-3); font-size: 12.5px; }
@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: .35; } }
@media (prefers-reduced-motion: reduce) { *, *::before, *::after { animation: none !important; transition: none !important; } }
@media (max-width: 900px) {
  .shell { grid-template-columns: 1fr; }
  .rail { border-right: 0; border-bottom: 1px solid var(--border); flex-direction: row; flex-wrap: wrap; gap: 16px; }
  .rail-foot { margin-top: 0; }
  .main { padding: 16px 14px 28px; }
}
</style>
</head>
<body>
<div class="shell">
  <aside class="rail">
    <div>
      <div class="brand">
        <span class="brand-dot" id="dot"></span>
        <span class="brand-name">taskflow</span>
      </div>
      <div class="brand-sub" id="subtitle">conectando…</div>
    </div>
    <div>
      <div class="label">Filas</div>
      <div id="queues"></div>
    </div>
    <div>
      <div class="label">Estados</div>
      <div class="state-legend" id="legend"></div>
    </div>
    <div class="rail-foot">
      <div id="journal">journal —</div>
      <div id="worker-info">workers —</div>
    </div>
  </aside>

  <main class="main">
    <header class="head">
      <div>
        <h1>Dashboard de tarefas</h1>
        <p id="headline">Aguardando o primeiro snapshot do broker…</p>
      </div>
      <span class="pill"><span class="mono" id="clock">--:--:--</span></span>
    </header>

    <section class="tiles" id="tiles"></section>

    <section class="panel">
      <div class="tabs" role="tablist">
        <button class="tab" role="tab" data-tab="tasks" aria-selected="true">Tasks <span class="badge PENDING" id="tab-count-tasks">0</span></button>
        <button class="tab" role="tab" data-tab="dlq" aria-selected="false">Dead letter <span class="badge FAILED" id="tab-count-dlq">0</span></button>
        <button class="tab" role="tab" data-tab="events" aria-selected="false">Eventos</button>
      </div>

      <div class="filters">
        <select id="filter-state" aria-label="Filtrar por estado">
          <option value="">todos os estados</option>
          <option value="PENDING">pendente</option>
          <option value="RUNNING">executando</option>
          <option value="RETRY">aguardando retry</option>
          <option value="SUCCESS">sucesso</option>
          <option value="FAILED">falha permanente</option>
          <option value="DEAD">dead letter</option>
        </select>
        <select id="filter-queue" aria-label="Filtrar por fila"><option value="">todas as filas</option></select>
        <input type="search" id="filter-text" placeholder="buscar por nome ou id" aria-label="Buscar task">
        <span class="spacer"></span>
        <span class="dim" id="last-update">—</span>
      </div>

      <div id="panel-tasks">
        <table>
          <thead><tr>
            <th>Task</th><th>Estado</th><th class="num">Tent.</th><th class="num">Prio</th>
            <th class="num">Duração</th><th>Fila</th><th>Worker</th><th>Detalhe</th>
          </tr></thead>
          <tbody id="rows-tasks"></tbody>
        </table>
        <div class="empty" id="empty-tasks" hidden>Nenhuma task ainda. Envie uma com <span class="mono">python -m taskflow.cli submit &lt;task&gt;</span>.</div>
      </div>

      <div id="panel-dlq" hidden>
        <table>
          <thead><tr>
            <th>Task</th><th>Estado</th><th class="num">Tent.</th><th>Fila</th>
            <th>Último erro</th><th class="num">Requeue</th>
          </tr></thead>
          <tbody id="rows-dlq"></tbody>
        </table>
        <div class="empty" id="empty-dlq" hidden>Dead letter queue vazia. </div>
      </div>

      <div id="panel-events" hidden>
        <div class="events" id="rows-events"></div>
      </div>
    </section>
  </main>
</div>

<script>
(function () {
  "use strict";

  var state = { data: null, tab: "tasks", connected: false, retries: 0 };
  var el = function (id) { return document.getElementById(id); };

  function escapeHtml(value) {
    return String(value === null || value === undefined ? "" : value)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function formatDuration(ms) {
    if (ms === null || ms === undefined) { return "—"; }
    if (ms < 1000) { return Math.round(ms) + "ms"; }
    if (ms < 60000) { return (ms / 1000).toFixed(1) + "s"; }
    return Math.floor(ms / 60000) + "m" + String(Math.floor((ms % 60000) / 1000)).padStart(2, "0") + "s";
  }

  function formatTime(ts) {
    if (!ts) { return "—"; }
    return new Date(ts * 1000).toLocaleTimeString("pt-BR", { hour12: false });
  }

  function shorten(text, limit) {
    var value = String(text === null || text === undefined ? "" : text).replace(/\\s+/g, " ");
    if (!value) { return "—"; }
    return value.length > limit ? value.slice(0, limit - 1) + "…" : value;
  }

  function badge(stateName) {
    return '<span class="badge ' + escapeHtml(stateName) + '">' + escapeHtml(stateName) + "</span>";
  }

  function detailOf(task) {
    var value = task.result ? task.result.value : null;
    if (task.state === "SUCCESS" && value !== null && value !== undefined) {
      return escapeHtml(shorten(JSON.stringify(value), 72));
    }
    if (task.result && task.result.error) {
      return '<span class="err">' + escapeHtml(shorten(task.result.error, 72)) + "</span>";
    }
    if (task.last_error) {
      return '<span class="err">' + escapeHtml(shorten(task.last_error, 72)) + "</span>";
    }
    if (task.eta && task.eta > Date.now() / 1000 && task.state === "RETRY") {
      return '<span class="dim">retry em ' + Math.max(0, Math.round(task.eta - Date.now() / 1000)) + "s</span>";
    }
    if (task.eta && task.eta > Date.now() / 1000 && task.state === "PENDING") {
      return '<span class="dim">começa em ' + Math.max(0, Math.round(task.eta - Date.now() / 1000)) + "s</span>";
    }
    return '<span class="dim">—</span>';
  }

  function filtered(list) {
    var wanted = el("filter-state").value;
    var queue = el("filter-queue").value;
    var text = el("filter-text").value.trim().toLowerCase();
    return list.filter(function (task) {
      if (wanted && task.state !== wanted) { return false; }
      if (queue && task.queue !== queue) { return false; }
      if (text && (task.name + " " + task.id).toLowerCase().indexOf(text) === -1) { return false; }
      return true;
    });
  }

  function renderTiles(stats) {
    var items = [
      { k: "prontas", v: stats.ready, s: "na fila, aguardando worker" },
      { k: "executando", v: stats.in_flight, s: "com lease ativa" },
      { k: "sucesso", v: stats.succeeded, s: "concluídas" },
      { k: "dead letter", v: stats.dead_lettered, s: "falharam de vez" },
      { k: "duração média", v: formatDuration(stats.avg_duration_ms), s: "das concluídas" },
      { k: "total", v: stats.total, s: "no ledger" }
    ];
    el("tiles").innerHTML = items.map(function (item) {
      return '<div class="tile"><div class="k">' + escapeHtml(item.k) + "</div>"
        + '<div class="v">' + escapeHtml(item.v) + "</div>"
        + '<div class="s">' + escapeHtml(item.s) + "</div></div>";
    }).join("");
  }

  function renderQueues(stats) {
    var select = el("filter-queue");
    var current = select.value;
    select.innerHTML = '<option value="">todas as filas</option>' + stats.queues.map(function (queue) {
      return '<option value="' + escapeHtml(queue.queue) + '">' + escapeHtml(queue.queue) + "</option>";
    }).join("");
    select.value = current;

    var all = stats.queues.reduce(function (acc, queue) { return acc + queue.total; }, 0);
    var blocks = ['<div class="queue-row' + (current ? "" : " active") + '" data-queue="">'
      + "<span>todas</span><span class=\\"count\\">" + all + "</span></div>"];
    stats.queues.forEach(function (queue) {
      blocks.push('<div class="queue-row' + (current === queue.queue ? " active" : "") + '" data-queue="'
        + escapeHtml(queue.queue) + '"><span>' + escapeHtml(queue.queue)
        + '</span><span class="count">' + queue.total + "</span></div>");
    });
    el("queues").innerHTML = blocks.join("");
    Array.prototype.forEach.call(document.querySelectorAll("#queues .queue-row"), function (row) {
      row.addEventListener("click", function () {
        el("filter-queue").value = row.getAttribute("data-queue");
        render();
      });
    });

    var legend = [
      ["PENDING", "prontas", stats.ready],
      ["RUNNING", "executando", stats.in_flight],
      ["SUCCESS", "sucesso", stats.succeeded],
      ["FAILED", "permanentes", stats.failed],
      ["DEAD", "dead letter", stats.queues.reduce(function (acc, q) { return acc + q.dead; }, 0)]
    ];
    el("legend").innerHTML = legend.map(function (item) {
      return '<div class="legend-row">' + badge(item[0]) + "<span>" + escapeHtml(item[1])
        + "</span><b>" + item[2] + "</b></div>";
    }).join("");
  }

  function renderTasks(data) {
    var rows = filtered(data.tasks || []);
    el("rows-tasks").innerHTML = rows.map(function (task) {
      return "<tr>"
        + '<td class="name" title="' + escapeHtml(task.name) + '">' + escapeHtml(task.name)
        + ' <span class="dim mono">' + escapeHtml(task.id.slice(0, 8)) + "</span></td>"
        + "<td>" + badge(task.state) + "</td>"
        + '<td class="num">' + task.attempts + "/" + task.max_retries + "</td>"
        + '<td class="num">' + task.priority + "</td>"
        + '<td class="num">' + formatDuration(task.duration_ms) + "</td>"
        + "<td>" + escapeHtml(task.queue) + "</td>"
        + '<td class="dim">' + escapeHtml(task.worker_id || "—") + "</td>"
        + "<td>" + detailOf(task) + "</td></tr>";
    }).join("");
    el("empty-tasks").hidden = rows.length > 0;
    el("tab-count-tasks").textContent = (data.tasks || []).length;
  }

  function renderDlq(data) {
    var rows = data.dlq || [];
    var readOnly = data.read_only;
    el("rows-dlq").innerHTML = rows.map(function (task) {
      return "<tr>"
        + '<td class="name" title="' + escapeHtml(task.name) + '">' + escapeHtml(task.name) + "</td>"
        + "<td>" + badge(task.state) + "</td>"
        + '<td class="num">' + task.attempts + "/" + task.max_retries + "</td>"
        + "<td>" + escapeHtml(task.queue) + "</td>"
        + '<td class="err">' + escapeHtml(shorten(task.last_error, 60)) + "</td>"
        + '<td class="num"><button class="btn small" data-requeue="' + escapeHtml(task.id) + '"'
        + (readOnly ? " disabled title=\\"outro processo está escrevendo neste data-dir\\"" : "")
        + ">reenfileirar</button></td></tr>";
    }).join("");
    el("empty-dlq").hidden = rows.length > 0;
    el("empty-dlq").textContent = readOnly
      ? "Dead letter queue vazia. (modo somente-leitura: outro processo segura a trava de escrita)"
      : "Dead letter queue vazia.";
    el("tab-count-dlq").textContent = rows.length;
    Array.prototype.forEach.call(document.querySelectorAll("[data-requeue]"), function (button) {
      button.addEventListener("click", function () {
        var id = button.getAttribute("data-requeue");
        button.disabled = true;
        fetch("/api/dlq/" + encodeURIComponent(id) + "/requeue", { method: "POST" })
          .then(function (response) { return response.json(); })
          .then(function () { if (state.data) { render(); } })
          .catch(function () { button.disabled = false; });
      });
    });
  }

  function renderEvents(data) {
    var rows = (data.events || []).slice().reverse();
    el("rows-events").innerHTML = rows.map(function (event) {
      var tone = { enqueued: "PENDING", started: "RUNNING", success: "SUCCESS", failed: "FAILED", dead: "DEAD" };
      return '<div class="event"><span class="t mono">' + formatTime(event.timestamp) + "</span>"
        + '<span class="tag">' + badge(tone[event.type] || "PENDING") + "</span>"
        + '<span class="msg">' + escapeHtml(event.name) + " "
        + '<span class="dim">' + escapeHtml(describeEvent(event)) + "</span></span></div>";
    }).join("") || '<div class="empty">Nenhum evento ainda.</div>';
  }

  function describeEvent(event) {
    var payload = event.payload || {};
    if (event.type === "started") { return "tentativa " + payload.attempt + " · " + (payload.worker_id || ""); }
    if (event.type === "success") { return formatDuration(payload.duration_ms) + " · tentativa " + payload.attempts; }
    if (event.type === "failed" && payload.will_retry) {
      return "tentativa " + payload.attempts + "/" + payload.max_retries + " · retry em " + payload.retry_in + "s";
    }
    if (event.type === "failed") { return shorten(payload.error, 80); }
    if (event.type === "dead") { return shorten(payload.error, 80); }
    if (payload.requeued) { return "reenfileirada (veio de " + payload.previous_state + ")"; }
    if (payload.recovered) { return "recuperada após worker morto"; }
    return "prio " + payload.priority;
  }

  function render() {
    var data = state.data;
    if (!data) { return; }
    var stats = data.stats || { queues: [] };
    renderTiles(stats);
    renderQueues(stats);
    renderTasks(data);
    renderDlq(data);
    renderEvents(data);
    el("headline").textContent = stats.total + " tasks · " + stats.ready + " prontas · "
      + stats.in_flight + " em execução · " + stats.dead_lettered + " na DLQ";
    el("last-update").textContent = "atualizado " + formatTime(data.generated_at);
    el("journal").textContent = "journal " + stats.journal_lines + " linhas";
    var workers = data.worker_stats;
    el("worker-info").textContent = workers
      ? workers.concurrency + " workers · " + workers.processed + " ok · " + workers.failures + " falhas"
      : (data.read_only ? "somente leitura" : "sem worker embutido");
    el("subtitle").textContent = data.read_only ? "somente leitura" : "escrevendo em " + (stats.queues.length) + " fila(s)";
  }

  function connect() {
    var protocol = window.location.protocol === "https:" ? "wss://" : "ws://";
    var socket = new WebSocket(protocol + window.location.host + "/ws");
    socket.onopen = function () { state.connected = true; state.retries = 0; setDot(); };
    socket.onclose = function () {
      state.connected = false; setDot();
      state.retries = Math.min(state.retries + 1, 6);
      window.setTimeout(connect, 300 * Math.pow(2, state.retries));
    };
    socket.onerror = function () { socket.close(); };
    socket.onmessage = function (message) {
      var payload = JSON.parse(message.data);
      if (payload.type === "snapshot") { state.data = payload.data; render(); }
    };
  }

  function setDot() {
    var dot = el("dot");
    dot.className = "brand-dot " + (state.connected ? "live" : "off");
  }

  Array.prototype.forEach.call(document.querySelectorAll(".tab"), function (tab) {
    tab.addEventListener("click", function () {
      state.tab = tab.getAttribute("data-tab");
      Array.prototype.forEach.call(document.querySelectorAll(".tab"), function (other) {
        other.setAttribute("aria-selected", other === tab ? "true" : "false");
      });
      ["tasks", "dlq", "events"].forEach(function (name) {
        el("panel-" + name).hidden = name !== state.tab;
      });
    });
  });

  ["filter-state", "filter-queue", "filter-text"].forEach(function (id) {
    el(id).addEventListener("input", render);
  });

  window.setInterval(function () {
    el("clock").textContent = new Date().toLocaleTimeString("pt-BR", { hour12: false });
  }, 1000);

  setDot();
  render();
  connect();
})();
</script>
</body>
</html>
"""