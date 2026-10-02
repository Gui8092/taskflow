"""Interface de linha de comando do taskflow.

Executável como ``python -m taskflow.cli <comando>``. Subcomandos disponíveis:

``submit``
    enfileira uma task registrada
``status``
    mostra o estado de uma task ou um resumo do broker
``tasks``
    lista as tasks registradas no processo
``monitor``
    visão ao vivo no terminal
``worker``
    sobe um pool de workers
``dashboard``
    sobe o dashboard web (FastAPI + WebSocket)
``dlq``
    inspeciona, reenfileira e limpa a dead letter queue
``cron``
    mostra as próximas execuções de uma expressão
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Sequence, TextIO

from taskflow.core.broker import Broker, BrokerError, BrokerLockedError
from taskflow.core.config import Config, ConfigError, setup_logging
from taskflow.core.events import Event, EventType
from taskflow.core.metrics import build_metrics_text
from taskflow.core.registry import RegistryError, TaskRegistry, build_registry
from taskflow.core.serialization import SerializationError
from taskflow.core.states import TaskState, describe_state
from taskflow.core.task import Task, TaskFilter
from taskflow.scheduler.cron import CronError, CronExpression
from taskflow.scheduler.scheduler import Scheduler
from taskflow.worker.deadletter import DeadLetterQueue
from taskflow.worker.pool import WorkerPool, worker_queues

PROGRAM: Final[str] = "taskflow"

#: Códigos de saída da CLI.
EXIT_OK: Final[int] = 0
EXIT_ERROR: Final[int] = 1
EXIT_USAGE: Final[int] = 2
EXIT_INTERRUPTED: Final[int] = 130

_STATE_STYLES: Final[dict[TaskState, tuple[str, str]]] = {
    TaskState.PENDING: ("\033[90m", "cinza"),
    TaskState.RUNNING: ("\033[35m", "violeta"),
    TaskState.SUCCESS: ("\033[32m", "verde"),
    TaskState.RETRY: ("\033[33m", "amarelo"),
    TaskState.FAILED: ("\033[31m", "vermelho"),
    TaskState.DEAD: ("\033[31;1m", "vermelho forte"),
    TaskState.CANCELLED: ("\033[36m", "ciano"),
}

_RESET: Final[str] = "\033[0m"
_BOLD: Final[str] = "\033[1m"
_DIM: Final[str] = "\033[90m"


class CliError(RuntimeError):
    """Erro de uso da CLI, reportado sem traceback."""


@dataclass(slots=True)
class Context:
    """Objeto que carrega o que os handlers precisam: config, registro, broker e saída."""

    config: Config
    registry: TaskRegistry
    broker: Broker
    out: TextIO
    color: bool

    def write(self, text: str = "") -> None:
        """Escreve uma linha na saída padrão."""
        print(text, file=self.out)

    def error(self, text: str) -> None:
        """Escreve uma mensagem de erro na saída de erro."""
        print(text, file=sys.stderr)


# ---------------------------------------------------------------------- apresentação


def supports_color(stream: TextIO, *, force: bool | None = None) -> bool:
    """Decide se vale a pena usar cores ANSI na saída."""
    if force is not None:
        return force
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("TERM", "") == "dumb":
        return False
    return bool(getattr(stream, "isatty", lambda: False)())


def paint(text: str, color: str, enabled: bool) -> str:
    """Aplica cor ANSI quando habilitada."""
    return f"{color}{text}{_RESET}" if enabled and color else text


def state_text(state: TaskState, enabled: bool) -> str:
    """Formata o estado de uma task com cor."""
    color = _STATE_STYLES.get(state, ("", ""))[0]
    return paint(f"{state.value:<8}", color, enabled)


def render_table(
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
    *,
    aligns: Sequence[str] | None = None,
    indent: str = "  ",
) -> str:
    """Monta uma tabela ASCII alinhada por coluna.

    Args:
        headers: Títulos das colunas.
        rows: Linhas (strings já formatadas).
        aligns: ``"l"`` ou ``"r"`` por coluna (padrão: ``"l"``).
        indent: Prefixo aplicado em todas as linhas.

    Returns:
        A tabela pronta para impressão (sem cor).
    """
    if not headers:
        return ""
    columns = len(headers)
    widths = [len(str(headers[index])) for index in range(columns)]
    for row in rows:
        for index in range(columns):
            widths[index] = max(widths[index], _display_width(str(row[index])))
    alignment = list(aligns or ["l"] * columns)
    while len(alignment) < columns:
        alignment.append("l")

    def line(cells: Sequence[str]) -> str:
        """Formata uma linha respeitando a largura de cada coluna."""
        parts = []
        for index in range(columns):
            cell = str(cells[index])
            padding = widths[index] - _display_width(cell)
            parts.append(" " * padding + cell if alignment[index] == "r" else cell + " " * padding)
        return indent + " ".join(parts).rstrip()

    top = indent + "+" + "+".join("-" * (width + 1) for width in widths) + "+"
    separator = indent + "+" + "+".join("=" * (width + 1) for width in widths) + "+"
    output = [top, line(headers), separator]
    for row in rows:
        output.append(line(row))
    output.append(top)
    return "\n".join(output)


def _display_width(text: str) -> int:
    """Largura aproximada de um texto já colorido (ignora códigos ANSI)."""
    length = 0
    index = 0
    while index < len(text):
        if text[index] == "\033":
            closing = text.find("m", index)
            if closing == -1:
                break
            index = closing + 1
            continue
        length += 1
        index += 1
    return length


def format_duration(value: float | None) -> str:
    """Formata milissegundos de forma legível (``142ms``, ``8.1s``, ``2m03s``)."""
    if value is None:
        return "-"
    if value < 1000:
        return f"{value:.0f}ms"
    seconds = value / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, rest = divmod(int(seconds), 60)
    return f"{minutes}m{rest:02d}s"


def format_timestamp(value: float | None) -> str:
    """Formata um instante epoch em hora local ``HH:MM:SS``."""
    if value is None:
        return "-"
    return datetime.fromtimestamp(value).strftime("%H:%M:%S")


def truncate(text: str | None, limit: int = 60) -> str:
    """Corta um texto longo, acrescentando reticências."""
    if not text:
        return "-"
    single_line = " ".join(text.split())
    if len(single_line) <= limit:
        return single_line
    return single_line[: limit - 1] + "..."


# ---------------------------------------------------------------------- tabela de tasks


def task_detail_text(task: Task) -> str | None:
    """Escolhe o que mostrar na coluna final: o retorno em caso de sucesso, o erro caso contrário."""
    if task.result is not None:
        if task.result.error:
            return task.result.error
        if task.result.value is not None:
            valor = task.result.value
            return valor if isinstance(valor, str) else json.dumps(valor, ensure_ascii=False)
    if task.state in {TaskState.RETRY, TaskState.PENDING} and task.last_error:
        return task.last_error
    return task.last_error


def task_row(task: Task, ctx: Context) -> list[str]:
    """Formata uma linha da tabela de tasks."""
    return [
        task.short_id,
        task.name,
        state_text(task.state, ctx.color),
        f"{task.attempts}/{task.max_retries}",
        str(task.priority),
        format_duration(task.duration_ms),
        format_timestamp(task.finished_at or task.started_at or task.enqueued_at),
        truncate(task_detail_text(task), 44),
    ]


TASK_HEADERS: Final[tuple[str, ...]] = (
    "ID",
    "TASK",
    "ESTADO",
    "TENT",
    "PRIO",
    "TEMPO",
    "QUANDO",
    "ERRO/VALOR",
)


def render_tasks(tasks: Sequence[Task], ctx: Context, *, title: str | None = None) -> str:
    """Renderiza a tabela de tasks com cabeçalho opcional."""
    blocks: list[str] = []
    if title:
        blocks.append(paint(title, _BOLD, ctx.color))
    if not tasks:
        blocks.append("  (nenhuma task)")
        return "\n".join(blocks)
    rows = [task_row(task, ctx) for task in tasks]
    blocks.append(render_table(TASK_HEADERS, rows, aligns=["l", "l", "l", "l", "r", "r", "l", "l"]))
    return "\n".join(blocks)


def render_task_detail(task: Task, ctx: Context) -> str:
    """Renderiza o detalhe de uma task (usado por ``status <id>``)."""
    color = _STATE_STYLES.get(task.state, ("", ""))[0]
    lines = [
        f"{paint('task', _BOLD, ctx.color)}    {task.name}",
        f"{paint('id', _BOLD, ctx.color)}      {task.id}",
        f"{paint('estado', _BOLD, ctx.color)}  {paint(task.state.value, color, ctx.color)} ({describe_state(task.state)})",
        f"{paint('fila', _BOLD, ctx.color)}    {task.queue}",
        f"{paint('prioridade', _BOLD, ctx.color)} {task.priority}",
        f"{paint('tentativas', _BOLD, ctx.color)} {task.attempts} de {task.max_retries} retries"
        + (f" (recuperada {task.recovered}x)" if task.recovered else ""),
        f"{paint('args', _BOLD, ctx.color)}    {json.dumps(task.args, ensure_ascii=False)}",
        f"{paint('kwargs', _BOLD, ctx.color)}  {json.dumps(task.kwargs, ensure_ascii=False)}",
        f"{paint('timeout', _BOLD, ctx.color)} {task.timeout if task.timeout is not None else 'sem timeout'}",
        f"{paint('criada', _BOLD, ctx.color)}  {format_timestamp(task.created_at)}",
        f"{paint('iniciada', _BOLD, ctx.color)} {format_timestamp(task.started_at)}",
        f"{paint('finalizada', _BOLD, ctx.color)} {format_timestamp(task.finished_at)}",
        f"{paint('duração', _BOLD, ctx.color)} {format_duration(task.duration_ms)}",
    ]
    if task.lease is not None:
        lines.append(f"{paint('lease', _BOLD, ctx.color)}   {task.lease.lease_id[:8]} por {task.lease.worker_id}")
    if task.last_error:
        lines.append(f"{paint('erro', _BOLD, ctx.color)}    {task.last_error}")
    if task.result is not None:
        if task.result.value is not None:
            lines.append(f"{paint('retorno', _BOLD, ctx.color)} {truncate(json.dumps(task.result.value, ensure_ascii=False), 200)}")
        if task.result.traceback:
            lines.append(f"{paint('traceback', _BOLD, ctx.color)}")
            lines.append(task.result.traceback.rstrip())
    return "\n".join(lines)


def render_stats(broker: Broker, ctx: Context) -> str:
    """Renderiza o resumo do broker (contagens por fila)."""
    stats = broker.stats()
    rows = [
        [
            item.queue,
            str(item.pending),
            str(item.retrying),
            str(item.running),
            str(item.succeeded),
            str(item.dead + item.failed),
        ]
        for item in stats.queues
    ]
    return render_table(
        ("FILA", "PEND", "RETRY", "RUN", "OK", "ERRO"),
        rows,
        aligns=["l", "r", "r", "r", "r", "r"],
    )


# ---------------------------------------------------------------------- parser


def build_parser() -> argparse.ArgumentParser:
    """Monta o parser de argumentos com todos os subcomandos."""
    parser = argparse.ArgumentParser(
        prog=PROGRAM,
        description="taskflow — fila de tarefas distribuída implementada do zero",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "exemplos:\n"
            "  taskflow submit send_email --args '[\"ana@exemplo.com\"]'\n"
            "  taskflow worker -c 4 --queue emails --drain\n"
            "  taskflow status --queue emails\n"
            "  taskflow monitor --once\n"
            "  taskflow dashboard --workers 2\n"
            "  taskflow dlq list / dlq requeue <id>\n"
        ),
    )
    parser.add_argument("--data-dir", help="diretório de estado (padrão: $TASKFLOW_DATA_DIR)")
    parser.add_argument("--modules", help="módulos separados por vírgula que registram as tasks")
    parser.add_argument("--log-level", help="nível de log (DEBUG, INFO, WARNING, ERROR)")
    parser.add_argument(
        "--no-lock",
        action="store_true",
        help="permite escrita concorrente no mesmo data-dir (o journal é append-only)",
    )
    parser.add_argument("--no-color", action="store_true", help="desliga as cores ANSI")

    subparsers = parser.add_subparsers(dest="command", metavar="COMANDO")

    submit = subparsers.add_parser("submit", help="enfileira uma task registrada")
    submit.add_argument("name", help="nome lógico da task")
    submit.add_argument("--args", help="argumentos posicionais em JSON (ex.: '[\"ana@exemplo.com\"]')")
    submit.add_argument("--kwargs", help="argumentos nomeados em JSON (ex.: '{\"n\": 2}')")
    submit.add_argument("--queue", help="fila de destino")
    submit.add_argument("--priority", type=int, help="prioridade (maior executa antes)")
    submit.add_argument("--max-retries", type=int, help="quantidade de retries")
    submit.add_argument("--timeout", type=float, help="timeout em segundos")
    submit.add_argument("--delay", type=float, help="segundos até a task ficar pronta")
    submit.add_argument("--tags", help="metadados em JSON (ex.: '{\"cliente\": \"ana\"}')")
    submit.add_argument("--wait", action="store_true", help="executa inline e espera o resultado")
    submit.add_argument("--wait-timeout", type=float, default=30.0, help="teto do --wait")

    status = subparsers.add_parser("status", help="estado de uma task ou resumo do broker")
    status.add_argument("id", nargs="?", help="id da task (opcional)")
    status.add_argument("--queue", help="filtra por fila")
    status.add_argument("--state", help="filtra por estado (PENDING, RUNNING, SUCCESS, RETRY, FAILED, DEAD)")
    status.add_argument("--limit", type=int, default=20, help="quantidade de linhas")
    status.add_argument("--json", action="store_true", help="saída em JSON")

    tasks_parser = subparsers.add_parser("tasks", help="lista as tasks registradas")
    tasks_parser.add_argument("--json", action="store_true", help="saída em JSON")

    cancel = subparsers.add_parser("cancel", help="cancela uma task que ainda está na fila")
    cancel.add_argument("id", help="id (ou prefixo) da task")
    cancel.add_argument("--reason", help="motivo registrado na task")

    metrics = subparsers.add_parser(
        "metrics", help="métricas no formato de texto do Prometheus"
    )
    metrics.add_argument("--json", action="store_true", help="saída em JSON em vez de texto")

    scheduler = subparsers.add_parser(
        "scheduler", help="roda o agendador periódico enfileirando tasks"
    )
    scheduler.add_argument("name", help="nome lógico da task a agendar")
    scheduler.add_argument("--cron", help="expressão cron (ex.: '*/5 * * * *')")
    scheduler.add_argument(
        "--interval", type=float, help="dispara a cada N segundos (permite menos de 1 minuto)"
    )
    scheduler.add_argument("--args", help="argumentos posicionais em JSON")
    scheduler.add_argument("--kwargs", help="argumentos nomeados em JSON")
    scheduler.add_argument("--queue", help="fila de destino")
    scheduler.add_argument("--priority", type=int, help="prioridade")
    scheduler.add_argument(
        "--workers",
        type=int,
        default=0,
        help="workers embutidos nesse processo (recomendado: o agendador segura a trava de escrita)",
    )
    scheduler.add_argument(
        "--max-runtime", type=float, help="encerra após N segundos (útil em smoke tests)"
    )

    monitor = subparsers.add_parser("monitor", help="visão ao vivo no terminal")
    monitor.add_argument("--once", action="store_true", help="imprime um quadro e sai")
    monitor.add_argument("--interval", type=float, help="segundos entre quadros")
    monitor.add_argument("--rows", type=int, help="quantidade de linhas")
    monitor.add_argument("--queue", help="filtra por fila")
    monitor.add_argument("--state", help="filtra por estado")

    worker = subparsers.add_parser("worker", help="sobe um pool de workers")
    worker.add_argument("-c", "--concurrency", type=int, help="quantidade de workers")
    worker.add_argument("--queue", action="append", help="fila a consumir (repetível)")
    worker.add_argument("--drain", action="store_true", help="encerra quando a fila esvaziar")
    worker.add_argument("--max-idle", type=float, help="encerra após N segundos sem trabalho")
    worker.add_argument("--max-runtime", type=float, help="encerra após N segundos de execução")

    dashboard = subparsers.add_parser("dashboard", help="sobe o dashboard web")
    dashboard.add_argument("--host", help="host do servidor")
    dashboard.add_argument("--port", type=int, help="porta do servidor")
    dashboard.add_argument("--workers", type=int, help="workers embutidos no dashboard")

    dlq = subparsers.add_parser("dlq", help="dead letter queue")
    dlq_sub = dlq.add_subparsers(dest="dlq_command", metavar="AÇÃO")
    dlq_list = dlq_sub.add_parser("list", help="lista as tasks mortas")
    dlq_list.add_argument("--queue", help="filtra por fila")
    dlq_list.add_argument("--limit", type=int, default=50, help="quantidade de linhas")
    dlq_list.add_argument("--json", action="store_true", help="saída em JSON")
    requeue_parser = dlq_sub.add_parser("requeue", help="reenfileira tasks da DLQ")
    requeue_parser.add_argument("id", nargs="?", help="id da task (omissão = todas)")
    requeue_parser.add_argument("--queue", help="fila de destino")
    requeue_parser.add_argument("--priority", type=int, help="nova prioridade")
    requeue_parser.add_argument("--limit", type=int, default=100, help="teto em modo 'todas'")
    requeue_parser.add_argument("--keep-attempts", action="store_true", help="não zera as tentativas")
    dlq_sub.add_parser("purge", help="remove as tasks da DLQ do ledger")

    cron = subparsers.add_parser("cron", help="expressões cron")
    cron_sub = cron.add_subparsers(dest="cron_command", metavar="AÇÃO")
    cron_next = cron_sub.add_parser("next", help="mostra as próximas execuções")
    cron_next.add_argument("expression", help="expressão cron (ex.: '*/5 * * * *')")
    cron_next.add_argument("--count", type=int, default=5, help="quantidade de execuções")
    cron_sub.add_parser("check", help="valida uma expressão cron").add_argument(
        "expression", help="expressão cron (ex.: '*/5 * * * *')"
    )
    return parser


def build_config(args: argparse.Namespace) -> Config:
    """Monta a configuração a partir do ambiente e das opções globais."""
    modules = tuple(
        part.strip() for part in (args.modules or "").split(",") if part.strip()
    )
    overrides: dict[str, Any] = {
        "modules": modules or None,
        "log_level": args.log_level,
        "lock_enabled": False if args.no_lock else None,
    }
    if args.data_dir:
        overrides["data_dir"] = args.data_dir
    return Config.from_env(**overrides)


# ---------------------------------------------------------------------- handlers


async def _cmd_submit(args: argparse.Namespace, ctx: Context) -> int:
    """``submit``: enfileira uma task e, com ``--wait``, executa inline."""
    positional = _parse_json(args.args, "args", default=[])
    keywords = _parse_json(args.kwargs, "kwargs", default={})
    tags = _parse_json(args.tags, "tags", default={})
    if not isinstance(positional, list):
        raise CliError("--args precisa ser uma lista JSON (ex.: '[\"ana@exemplo.com\"]')")
    if not isinstance(keywords, dict):
        raise CliError("--kwargs precisa ser um objeto JSON (ex.: '{\"n\": 2}')")
    if not isinstance(tags, dict):
        raise CliError("--tags precisa ser um objeto JSON (ex.: '{\"cliente\": \"ana\"}')")
    task = await ctx.broker.submit(
        args.name,
        *positional,
        **keywords,
        queue=args.queue,
        priority=args.priority,
        max_retries=args.max_retries,
        timeout=args.timeout,
        delay=args.delay,
        tags={str(key): str(value) for key, value in tags.items()},
    )
    ctx.write(paint(f"task enfileirada {task.short_id}", _BOLD, ctx.color))
    ctx.write(f"  nome      {task.name}")
    ctx.write(f"  fila      {task.queue}")
    ctx.write(f"  prioridade {task.priority}")
    ctx.write(f"  retries   {task.max_retries}")
    if task.eta and task.eta > time.time():
        ctx.write(f"  pronta em {task.eta - time.time():.1f}s")
    ctx.write(f"  id        {task.id}")

    if not args.wait:
        return EXIT_OK

    pool = WorkerPool(
        ctx.broker,
        ctx.registry,
        ctx.config,
        concurrency=1,
        queues=(task.queue,),
        worker_prefix="inline",
    )
    deadline = time.monotonic() + args.wait_timeout
    while time.monotonic() < deadline:
        current = ctx.broker.get(task.id)
        if current is not None and current.state.is_terminal:
            break
        await pool.process_one(timeout=0.2)
    current = ctx.broker.get(task.id)
    if current is None:
        ctx.error("task desapareceu do ledger")
        return EXIT_ERROR
    ctx.write("")
    ctx.write(render_task_detail(current, ctx))
    return EXIT_OK if current.state is TaskState.SUCCESS else EXIT_ERROR


async def _cmd_status(args: argparse.Namespace, ctx: Context) -> int:
    """``status``: detalhe de uma task ou resumo do broker."""
    if args.id:
        task = ctx.broker.get(args.id)
        if task is None:
            for candidate in ctx.broker.list_tasks(TaskFilter(limit=0)):
                if candidate.id.startswith(args.id):
                    task = candidate
                    break
        if task is None:
            raise CliError(f"task não encontrada: {args.id}")
        if args.json:
            ctx.write(json.dumps(task.summary(), ensure_ascii=False, indent=2))
        else:
            ctx.write(render_task_detail(task, ctx))
        return EXIT_OK if task.state is TaskState.SUCCESS else EXIT_ERROR
    state = TaskState.coerce(args.state) if args.state else None
    tasks = ctx.broker.list_tasks(TaskFilter(state=state, queue=args.queue, limit=args.limit))
    if args.json:
        ctx.write(json.dumps([task.summary() for task in tasks], ensure_ascii=False, indent=2))
        return EXIT_OK
    ctx.write(render_stats(ctx.broker, ctx))
    ctx.write("")
    ctx.write(render_tasks(tasks, ctx, title=f"tasks ({len(tasks)})"))
    return EXIT_OK


async def _cmd_tasks(args: argparse.Namespace, ctx: Context) -> int:
    """``tasks``: lista as tasks registradas neste processo."""
    described = ctx.registry.describe()
    if args.json:
        ctx.write(json.dumps(described, ensure_ascii=False, indent=2))
        return EXIT_OK
    if not described:
        ctx.write(
            "nenhuma task registrada; use --modules para importar módulos com o decorador @task"
        )
        return EXIT_OK
    rows = [
        [
            item["name"],
            item["queue"],
            str(item["priority"]),
            str(item["max_retries"]),
            "async" if item["is_async"] else "sync",
            item["signature"],
        ]
        for item in described
    ]
    ctx.write(
        render_table(
            ("NOME", "FILA", "PRIO", "RETRIES", "TIPO", "ASSINATURA"),
            rows,
            aligns=["l", "l", "r", "r", "l", "l"],
        )
    )
    return EXIT_OK


async def _cmd_cancel(args: argparse.Namespace, ctx: Context) -> int:
    """``cancel``: cancela uma task que ainda está na fila."""
    task = ctx.broker.get(args.id)
    if task is None:
        for candidate in ctx.broker.list_tasks(TaskFilter(limit=0)):
            if candidate.id.startswith(args.id):
                task = candidate
                break
    if task is None:
        raise CliError(f"task não encontrada: {args.id}")
    cancelada = await ctx.broker.cancel(task.id, reason=args.reason)
    ctx.write(paint(f"task cancelada {cancelada.short_id}", _BOLD, ctx.color))
    ctx.write(f"  nome    {cancelada.name}")
    ctx.write(f"  fila    {cancelada.queue}")
    if args.reason:
        ctx.write(f"  motivo  {args.reason}")
    return EXIT_OK


async def _cmd_metrics(args: argparse.Namespace, ctx: Context) -> int:
    """``metrics``: imprime as métricas do broker."""
    ctx.broker.refresh()
    if args.json:
        ctx.write(json.dumps(ctx.broker.stats().to_dict(), ensure_ascii=False, indent=2))
        return EXIT_OK
    ctx.write(build_metrics_text(ctx.broker), )
    return EXIT_OK


async def _cmd_scheduler(args: argparse.Namespace, ctx: Context) -> int:
    """``scheduler``: enfileira uma task periodicamente até ser interrompido."""
    if bool(args.cron) == bool(args.interval):
        raise CliError("informe exatamente um: --cron EXPRESSAO ou --interval SEGUNDOS")
    positional = _parse_json(args.args, "args", default=[])
    keywords = _parse_json(args.kwargs, "kwargs", default={})
    if not isinstance(positional, list) or not isinstance(keywords, dict):
        raise CliError("--args precisa ser uma lista e --kwargs um objeto JSON")

    agendador = Scheduler(ctx.broker, ctx.config)
    if args.cron:
        schedule = agendador.add_task(
            args.name, args.cron, *positional, **keywords, queue=args.queue, priority=args.priority
        )
    else:
        schedule = agendador.add_interval(
            args.name,
            args.interval,
            *positional,
            **keywords,
            queue=args.queue,
            priority=args.priority,
        )

    pool = None
    if args.workers > 0:
        pool = WorkerPool(
            ctx.broker,
            ctx.registry,
            ctx.config,
            concurrency=args.workers,
            worker_prefix="scheduler",
        )

    ctx.write(paint(f"{PROGRAM} scheduler", _BOLD, ctx.color))
    ctx.write(f"  task       {schedule.name}")
    ctx.write(f"  gatilho    {schedule.expression}")
    ctx.write(f"  fila       {schedule.queue}")
    ctx.write(f"  workers    {args.workers if pool else 'nenhum (só enfileira)'}")
    if not pool:
        ctx.write(
            paint(
                "  aviso      este processo segura a trava de escrita; "
                "enfileirar de outro processo no mesmo data-dir vai falhar. "
                "Use --workers N para consumir aqui dentro.",
                _DIM,
                ctx.color,
            )
        )
    ctx.write(paint("  Ctrl+C para encerrar", _DIM, ctx.color))

    if pool is not None:
        await pool.start()
    await agendador.start()
    inicio = time.monotonic()
    try:
        while True:
            await asyncio.sleep(0.5)
            proximo = agendador.next_runs()
            ctx.write(
                paint(
                    f"  {format_timestamp(time.time())}  disparos={schedule.run_count}  "
                    f"proximo={format_timestamp(proximo[0]['next_run_at']) if proximo else '-'}",
                    _DIM,
                    ctx.color,
                )
            )
            if args.max_runtime is not None and time.monotonic() - inicio >= args.max_runtime:
                ctx.write(paint(f"  tempo máximo de {args.max_runtime}s atingido", _DIM, ctx.color))
                break
    except asyncio.CancelledError:
        ctx.write(paint("  interrompido", _DIM, ctx.color))
    finally:
        await agendador.stop()
        if pool is not None:
            await pool.stop(drain=False, timeout=5.0)
    ctx.write(f"resumo: {schedule.run_count} disparo(s), {schedule.error_count} erro(s)")
    return EXIT_OK


async def _cmd_monitor(args: argparse.Namespace, ctx: Context) -> int:
    """``monitor``: quadro ao vivo do broker, redesenhado com sequências ANSI."""
    ctx.broker.refresh()
    previous = 0
    events: list[Event] = []
    ctx.broker.events.subscribe(events.append, name="monitor")
    interval = args.interval or ctx.config.monitor_interval
    rows = args.rows or ctx.config.monitor_rows
    state = TaskState.coerce(args.state) if args.state else None
    terminal_width = min(shutil.get_terminal_size((120, 24)).columns, 200)

    while True:
        ctx.broker.refresh()
        stats = ctx.broker.stats()
        tasks = ctx.broker.list_tasks(TaskFilter(state=state, queue=args.queue, limit=rows))
        title = (
            f"{PROGRAM} monitor · filas: {', '.join(ctx.broker.queue_names())} · "
            f"{datetime.now().strftime('%H:%M:%S')} · journal {stats.journal_lines} linhas"
        )
        counters = (
            f"PEND {stats.queued}  RUN {stats.in_flight}  OK {stats.succeeded}  "
            f"ERRO {stats.dead_lettered}  total {stats.total}"
        )
        body = [paint(title, _BOLD, ctx.color), paint(counters, _DIM, ctx.color), ""]
        body.append(render_tasks(tasks, ctx))
        if events:
            body.append("")
            body.append(paint("eventos", _BOLD, ctx.color))
            for event in events[-4:]:
                body.append(f"  {format_timestamp(event.timestamp)} {describe_event(event, ctx.color)}")
        text = "\n".join(line[:terminal_width] for line in body)
        _write_live(ctx.out, text, previous, ansi=ctx.color)
        previous = len(text.splitlines()) if ctx.color else 0
        if args.once:
            return EXIT_OK
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return EXIT_INTERRUPTED


def _write_live(stream: TextIO, text: str, previous_lines: int, *, ansi: bool = True) -> None:
    """Redesenha um quadro.

    Com ``ansi=True`` sobe ``previous_lines`` linhas e limpa cada uma (terminal
    interativo). Sem ANSI — pipes, arquivos e ``--once`` em teste — imprime o quadro
    inteiro, para a saída ficar determinística.
    """
    if not ansi:
        stream.write(text + "\n")
        stream.flush()
        return
    prefix = f"\033[{previous_lines}A" if previous_lines else ""
    payload = "".join(f"\033[2K{line}\n" for line in text.splitlines())
    stream.write(prefix + payload)
    stream.flush()


def describe_event(event: Event, color: bool = False) -> str:
    """Formata um evento em uma linha curta para CLI e logs."""
    payload = event.payload or {}
    if event.type is EventType.ENQUEUED:
        extra = f"prio {payload.get('priority')}"
        if payload.get("requeued"):
            extra = f"requeue de {payload.get('previous_state')} · {extra}"
        if payload.get("recovered"):
            extra = f"recuperada · {extra}"
        return paint(event.type.value, _STATE_STYLES[TaskState.PENDING][0], color) + f" {event.name} ({extra})"
    if event.type is EventType.STARTED:
        return paint(event.type.value, _STATE_STYLES[TaskState.RUNNING][0], color) + (
            f" {event.name} tentativa {payload.get('attempt')} por {payload.get('worker_id')}"
        )
    if event.type is EventType.SUCCESS:
        return paint(event.type.value, _STATE_STYLES[TaskState.SUCCESS][0], color) + (
            f" {event.name} em {format_duration(payload.get('duration_ms'))}"
        )
    if event.type is EventType.FAILED:
        if payload.get("will_retry"):
            return paint("retry", _STATE_STYLES[TaskState.RETRY][0], color) + (
                f" {event.name} tentativa {payload.get('attempt')}/{payload.get('max_retries')}"
                f" em {payload.get('retry_in')}s — {truncate(payload.get('error'), 60)}"
            )
        return paint("failed", _STATE_STYLES[TaskState.FAILED][0], color) + (
            f" {event.name} {truncate(payload.get('error'), 70)}"
        )
    return paint(event.type.value, _STATE_STYLES[TaskState.DEAD][0], color) + (
        f" {event.name} após {payload.get('attempts')} tentativas — {truncate(payload.get('error'), 60)}"
    )


async def _cmd_worker(args: argparse.Namespace, ctx: Context) -> int:
    """``worker``: sobe o pool e consome as filas até parar."""
    queues = worker_queues(ctx.config, args.queue)
    pool = WorkerPool(
        ctx.broker,
        ctx.registry,
        ctx.config,
        concurrency=args.concurrency or ctx.config.worker_concurrency,
        queues=queues,
    )
    ctx.write(paint(f"{PROGRAM} worker", _BOLD, ctx.color))
    ctx.write(f"  {pool.describe()}")
    ctx.write(f"  data-dir {ctx.config.data_dir}")
    ctx.write(f"  journal  {ctx.config.journal_path}")
    await pool.start()
    started_at = time.monotonic()
    idle_since: float | None = None
    ctx.write(paint("  pronto — Ctrl+C para encerrar", _DIM, ctx.color))

    try:
        while True:
            await asyncio.sleep(0.2)
            stats = ctx.broker.stats()
            busy = pool.inflight > 0 or stats.queued > 0
            if busy:
                idle_since = None
            else:
                idle_since = idle_since or time.monotonic()
            if args.drain and not busy:
                ctx.write(
                    paint(
                        f"fila vazia — {pool.processed} processada(s), {pool.failures} falha(s)",
                        _DIM,
                        ctx.color,
                    )
                )
                break
            if args.max_idle is not None and idle_since is not None:
                if time.monotonic() - idle_since >= args.max_idle:
                    ctx.write(paint(f"ocioso há {args.max_idle}s — encerrando", _DIM, ctx.color))
                    break
            if args.max_runtime is not None and time.monotonic() - started_at >= args.max_runtime:
                ctx.write(paint(f"tempo máximo de {args.max_runtime}s atingido", _DIM, ctx.color))
                break
    except asyncio.CancelledError:
        ctx.write(paint("interrompido", _DIM, ctx.color))
    finally:
        await pool.stop(drain=True, timeout=10.0)
    stats = ctx.broker.stats()
    ctx.write(
        f"resumo: {pool.processed} ok · {pool.failures} falhas · "
        f"{stats.succeeded} concluídas · {stats.dead_lettered} na DLQ"
    )
    return EXIT_OK


async def _cmd_dashboard(args: argparse.Namespace, ctx: Context) -> int:
    """``dashboard``: sobe o servidor FastAPI com WebSocket."""
    import uvicorn

    from taskflow.dashboard.app import create_app

    workers = args.workers if args.workers is not None else ctx.config.dashboard_workers
    app = create_app(
        config=ctx.config,
        registry=ctx.registry,
        start_workers=workers if workers and workers > 0 else None,
        broker=ctx.broker,  # reaproveita o broker da CLI (que já detém a trava de escrita)
    )
    host = args.host or ctx.config.dashboard_host
    port = args.port or ctx.config.dashboard_port
    ctx.write(paint(f"{PROGRAM} dashboard", _BOLD, ctx.color))
    ctx.write(f"  url        http://{host}:{port}")
    ctx.write(f"  websocket  ws://{host}:{port}/ws")
    ctx.write(f"  data-dir   {ctx.config.data_dir}")
    ctx.write(f"  workers    {'embutidos: ' + str(workers) if workers else 'observando apenas'}")
    ctx.write(paint("  Ctrl+C para encerrar", _DIM, ctx.color))

    # O servidor roda no mesmo event loop da CLI (uvicorn.run criaria outro).
    servidor = uvicorn.Server(
        uvicorn.Config(
            app,
            host=host,
            port=port,
            log_level=ctx.config.log_level.lower(),
            lifespan="on",
        )
    )
    await servidor.serve()
    return EXIT_OK


async def _cmd_dlq(args: argparse.Namespace, ctx: Context) -> int:
    """``dlq``: lista, reenfileira ou limpa a dead letter queue."""
    action = args.dlq_command or "list"
    fila = getattr(args, "queue", None)
    dlq = DeadLetterQueue(ctx.broker)
    if action == "list":
        tasks = dlq.list(queue=fila, limit=args.limit)
        if args.json:
            ctx.write(json.dumps([task.summary() for task in tasks], ensure_ascii=False, indent=2))
            return EXIT_OK
        reasons = dlq.reasons()
        ctx.write(
            paint(
                f"dead letter queue: {reasons['DEAD']} esgotaram retries · "
                f"{reasons['FAILED']} falha permanente",
                _BOLD,
                ctx.color,
            )
        )
        ctx.write("")
        ctx.write(render_tasks(tasks, ctx, title=f"tasks mortas ({len(tasks)})"))
        return EXIT_OK
    if action == "requeue":
        reset = not args.keep_attempts
        if args.id:
            task = await dlq.requeue(
                args.id,
                queue=fila,
                priority=args.priority,
                reset_attempts=reset,
            )
            ctx.write(paint(f"reenfileirada {task.short_id} na fila {task.queue}", _BOLD, ctx.color))
            return EXIT_OK
        requeued = await dlq.requeue_all(
            queue=fila, limit=args.limit, reset_attempts=reset
        )
        ctx.write(paint(f"{len(requeued)} task(s) reenfileirada(s)", _BOLD, ctx.color))
        for task in requeued:
            ctx.write(f"  {task.short_id} {task.name} -> {task.queue}")
        return EXIT_OK
    if action == "purge":
        removed = await dlq.purge(queue=fila)
        ctx.write(paint(f"{removed} task(s) removida(s) da DLQ", _BOLD, ctx.color))
        return EXIT_OK
    raise CliError(f"ação de dlq desconhecida: {action}")


async def _cmd_cron(args: argparse.Namespace, ctx: Context) -> int:
    """``cron``: valida expressões e mostra as próximas execuções."""
    action = args.cron_command or "check"
    if action == "check":
        expression = getattr(args, "expression", None)
        if not expression:
            raise CliError("informe a expressão: taskflow cron check '*/5 * * * *'")
        cron = CronExpression.parse(expression)
        ctx.write(paint(f"expressão válida: {expression}", _BOLD, ctx.color))
        ctx.write(f"  {cron.describe()}")
        return EXIT_OK
    cron = CronExpression.parse(args.expression)
    ctx.write(paint(f"{args.expression} -> próximas {args.count} execuções", _BOLD, ctx.color))
    moment = datetime.now().replace(second=0, microsecond=0)
    for index in range(args.count):
        moment = cron.next_after(moment)
        delta = moment - datetime.now()
        segundos = int(delta.total_seconds())
        ctx.write(
            f"  {moment.strftime('%Y-%m-%d %H:%M')}  "
            + paint(f"em {segundos // 60}min {segundos % 60}s", _DIM, ctx.color)
        )
    return EXIT_OK


_HANDLERS = {
    "submit": (_cmd_submit, False),
    "status": (_cmd_status, True),
    "tasks": (_cmd_tasks, True),
    "cancel": (_cmd_cancel, False),
    "metrics": (_cmd_metrics, True),
    "scheduler": (_cmd_scheduler, False),
    "monitor": (_cmd_monitor, True),
    "worker": (_cmd_worker, False),
    "dashboard": (_cmd_dashboard, False),
    "dlq": (_cmd_dlq, False),
    "cron": (_cmd_cron, True),
}


def _parse_json(raw: str | None, label: str, *, default: Any) -> Any:
    """Faz o parse de uma opção JSON da CLI com mensagem de erro amigável."""
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CliError(f"--{label} não é um JSON válido: {exc.msg} (posição {exc.pos})") from exc


async def _run(args: argparse.Namespace, out: TextIO, err: TextIO) -> int:
    """Monta o contexto, executa o subcomando e fecha tudo."""
    command = args.command
    if not command:
        raise CliError("informe um subcomando (use --help para ver a lista)")
    if command == "dlq" and not args.dlq_command:
        raise CliError("informe a ação do dlq: list, requeue ou purge")
    if command == "cron" and not args.cron_command:
        raise CliError("informe a ação do cron: next ou check")
    handler, read_only = _HANDLERS[command]
    try:
        config = build_config(args)
    except ConfigError as exc:
        print(f"{PROGRAM}: {exc}", file=err)
        return EXIT_USAGE
    try:
        registry = build_registry(config.modules)
    except RegistryError as exc:
        print(f"{PROGRAM}: {exc}", file=err)
        return EXIT_ERROR
    broker = Broker(config, registry, read_only=read_only)
    ctx = Context(
        config=config,
        registry=registry,
        broker=broker,
        out=out,
        color=(not args.no_color) and supports_color(out),
    )
    try:
        await broker.start()
    except BrokerLockedError as exc:
        print(f"{PROGRAM}: {exc}", file=err)
        return EXIT_ERROR
    try:
        return await handler(args, ctx)
    finally:
        await broker.stop()


def main(argv: Sequence[str] | None = None, *, out: TextIO | None = None, err: TextIO | None = None) -> int:
    """Ponto de entrada da CLI.

    Args:
        argv: Argumentos (padrão: ``sys.argv[1:]``).
        out: Stream de saída (padrão: :data:`sys.stdout`).
        err: Stream de erro (padrão: :data:`sys.stderr`).

    Returns:
        O código de saída do processo.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    stdout = out if out is not None else sys.stdout
    stderr = err if err is not None else sys.stderr
    if args.command:
        setup_logging(args.log_level or os.environ.get("TASKFLOW_LOG_LEVEL") or "WARNING")
    try:
        return asyncio.run(_run(args, stdout, stderr))
    except KeyboardInterrupt:
        print(f"\n{PROGRAM}: interrompido", file=stderr)
        return EXIT_INTERRUPTED
    except CliError as exc:
        print(f"{PROGRAM}: {exc}", file=stderr)
        return EXIT_USAGE
    except (
        BrokerError,
        RegistryError,
        SerializationError,
        CronError,
        ConfigError,
        ValueError,
    ) as exc:
        print(f"{PROGRAM}: {type(exc).__name__}: {exc}", file=stderr)
        return EXIT_ERROR


__all__ = [
    "EXIT_ERROR",
    "EXIT_INTERRUPTED",
    "EXIT_OK",
    "EXIT_USAGE",
    "Context",
    "build_config",
    "build_parser",
    "format_duration",
    "main",
    "render_stats",
    "render_table",
    "render_task_detail",
    "render_tasks",
]