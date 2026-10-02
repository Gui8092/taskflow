"""Broker de tasks: filas em memória com prioridade e persistência em disco.

Persistência
------------
Cada mudança de estado vira **uma linha JSON** em um journal *append-only*
(``journal.jsonl``). A linha carrega a task inteira, de forma que o estado atual
é o resultado de aplicar as linhas em ordem (last-write-wins por id). Quando o
journal cresce demais, um snapshot atômico (``snapshot.json``) é escrito e o
journal é truncado — a recovery lê o snapshot e reaplica apenas as linhas
posteriores a ele.

Múltiplas filas
---------------
Cada fila tem sua própria heap ordenada por ``(-prioridade, sequência)``: dentro
da mesma prioridade vale a ordem de chegada (FIFO), e prioridade maior sempre
vence. O descobrimento de uma task usa ``asyncio.Condition`` por fila, com tempo
de espera limitado pelo menor ``eta`` pendente, para que uma task em retry não
segure a fila.

Execução duplicada
------------------
``fetch`` entrega a task com uma :class:`~taskflow.core.task.Lease` e incrementa
``attempts``. ``ack``/``nack`` exigem o ``lease_id`` correto (*fencing token*):
um worker que ficou lento demais, teve sua lease recolhida e a task reexecutada
por outro worker não consegue gravar estado. Leases vencidas são devolvidas à
fila pelo varredor de :meth:`Broker.reclaim_expired`.
"""

from __future__ import annotations

import asyncio
import contextlib
import heapq
import itertools
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, Mapping
from uuid import uuid4

from taskflow.core.config import Config
from taskflow.core.events import EventBus, EventType, make_event
from taskflow.core.registry import TaskRegistry
from taskflow.core.serialization import SerializationError, to_jsonable
from taskflow.core.states import TaskState
from taskflow.core.task import QUEUEABLE_STATES, Lease, Task, TaskFilter, TaskResult, new_task_id

LOGGER: Final[logging.Logger] = logging.getLogger("taskflow.broker")

#: Tamanho do histórico de eventos enviado no payload dos eventos.
PREVIEW_LIMIT: Final[int] = 200

#: Intervalo máximo que um worker espera antes de reavaliar a fila.
_MAX_FETCH_SLICE: Final[float] = 1.0

#: Versão do formato do snapshot, para futuras migrações.
SNAPSHOT_VERSION: Final[int] = 1


class BrokerError(RuntimeError):
    """Erro genérico do broker."""


class TaskNotFoundError(BrokerError, KeyError):
    """A task solicitada não existe no ledger do broker."""

    def __str__(self) -> str:
        """Mensagem legível (sem as aspas extras de ``KeyError``)."""
        return self.args[0] if self.args else "task não encontrada"


class ReadOnlyBrokerError(BrokerError):
    """Operação de escrita tentada em um broker aberto somente para leitura."""


class BrokerLockedError(BrokerError):
    """Outro processo já escreve neste diretório de estado."""


class StaleLeaseError(BrokerError):
    """A lease apresentada no ``ack``/``nack`` não é mais a lease corrente da task."""


def _preview(value: Any) -> str:
    """Trunca um valor para uma prévia textual nos eventos."""
    if value is None:
        return ""
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    if len(text) > PREVIEW_LIMIT:
        return text[: PREVIEW_LIMIT - 1] + "…"
    return text


def _read_pid(path: Path) -> str:
    """Lê o PID gravado no arquivo de trava (para a mensagem de erro)."""
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip() or "desconhecido"
    except OSError:
        return "desconhecido"


class _WriterLock:
    """Trava de escrita exclusiva por diretório de estado.

    Usa ``msvcrt.locking`` no Windows e ``fcntl.flock`` no POSIX. A trava pertence
    ao handle do arquivo, portanto dois handles no mesmo processo também conflitam
    — o que torna a garantia testável sem depender de outro processo.
    """

    def __init__(self, path: Path) -> None:
        """Abre o arquivo de trava e tenta aquisição imediata (não bloqueante)."""
        try:
            handle = open(path, "a+b")
        except OSError as exc:
            raise BrokerError(f"não foi possível abrir a trava {path}: {exc}") from exc
        if handle.seek(0, os.SEEK_END) == 0:
            handle.write(b"\n")
            handle.flush()
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            owner = _read_pid(path)
            handle.close()
            raise BrokerLockedError(
                f"outro processo já está escrevendo em {path.parent} (pid {owner}). "
                "Use outro --data-dir, ou --no-lock / TASKFLOW_LOCK=0 para permitir "
                "escrita concorrente (o journal é append-only)."
            ) from exc
        with contextlib.suppress(OSError):
            handle.seek(0)
            handle.truncate()
            handle.write(f"{os.getpid()}\n".encode("utf-8"))
            handle.flush()
        self._path = path
        self._handle: Any = handle

    def release(self) -> None:
        """Libera a trava e fecha o arquivo."""
        handle = getattr(self, "_handle", None)
        if handle is None:
            return
        with contextlib.suppress(OSError):
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
        self._handle = None


@dataclass(slots=True)
class _Queue:
    """Estado interno de uma fila."""

    name: str
    heap: list[tuple[int, int, str]] = field(default_factory=list)
    cond: asyncio.Condition = field(default_factory=asyncio.Condition)


@dataclass(frozen=True, slots=True)
class QueueStats:
    """Contagem de tasks por estado em uma fila."""

    queue: str
    pending: int
    retrying: int
    running: int
    succeeded: int
    failed: int
    dead: int

    @property
    def total(self) -> int:
        """Total de tasks conhecidas desta fila."""
        return self.pending + self.retrying + self.running + self.succeeded + self.failed + self.dead

    @property
    def queued(self) -> int:
        """Tasks aguardando execução (pendentes ou em retry)."""
        return self.pending + self.retrying

    def to_dict(self) -> dict[str, Any]:
        """Converte para um dicionário JSON-safe."""
        return {
            "queue": self.queue,
            "pending": self.pending,
            "retrying": self.retrying,
            "running": self.running,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "dead": self.dead,
            "queued": self.queued,
            "total": self.total,
        }


@dataclass(frozen=True, slots=True)
class BrokerStats:
    """Fotografia agregada do broker."""

    queues: tuple[QueueStats, ...]
    total: int
    ready: int
    queued: int
    in_flight: int
    dead_lettered: int
    succeeded: int
    failed: int
    started_at: float
    uptime_seconds: float
    journal_lines: int

    def to_dict(self) -> dict[str, Any]:
        """Converte para um dicionário JSON-safe."""
        return {
            "queues": [item.to_dict() for item in self.queues],
            "total": self.total,
            "ready": self.ready,
            "queued": self.queued,
            "in_flight": self.in_flight,
            "dead_lettered": self.dead_lettered,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "started_at": self.started_at,
            "uptime_seconds": round(self.uptime_seconds, 3),
            "journal_lines": self.journal_lines,
        }


class Broker:
    """Broker de tasks com filas priorizadas e persistência em journal."""

    def __init__(
        self,
        config: Config | None = None,
        registry: TaskRegistry | None = None,
        events: EventBus | None = None,
        *,
        read_only: bool = False,
    ) -> None:
        """Cria o broker.

        Args:
            config: Configuração; se omitida, usa a configuração do processo.
            registry: Registro usado para validar e resolver nomes de task.
            events: Barramento de eventos; um novo é criado se omitido.
            read_only: Quando ``True``, o broker apenas lê o estado em disco
                (usado por ``status``, ``monitor`` e ``dlq list``).
        """
        self.config: Final[Config] = config if config is not None else Config.from_env()
        self.registry: Final[TaskRegistry] = registry if registry is not None else TaskRegistry()
        self.events: Final[EventBus] = events if events is not None else EventBus()
        self.read_only: Final[bool] = read_only
        self._tasks: dict[str, Task] = {}
        self._queues: dict[str, _Queue] = {}
        self._index: dict[str, str] = {}
        self._sequence = itertools.count()
        self._journal: Any = None
        self._journal_lines = 0
        self._journal_offset = 0
        self._lock: _WriterLock | None = None
        self._reclaim_task: asyncio.Task[None] | None = None
        self._started_at: float = time.time()
        self._started = False

    # ------------------------------------------------------------------ ciclo de vida

    async def start(self) -> None:
        """Abre o journal, recupera o estado persistido e inicia o varredor de leases.

        Raises:
            BrokerLockedError: Se outro processo já escreve neste ``data_dir``.
        """
        if self._started:
            return
        self.config.ensure_dirs()
        if self.read_only:
            self._journal_lines = self._load_from_disk()
        else:
            self._lock = _WriterLock(self.config.lock_path) if self.config.lock_enabled else None
            self._journal_lines = self._load_from_disk()
            if self.config.recover_running_on_start:
                self._recover_running_tasks()
            self._journal = self.config.journal_path.open("a", encoding="utf-8", newline="\n")
        self._started_at = time.time()
        self._started = True
        if not self.read_only:
            self._reclaim_task = asyncio.create_task(self._reclaim_loop(), name="taskflow:reclaim")
        LOGGER.info(
            "broker iniciado",
            extra={
                "data_dir": str(self.config.data_dir),
                "tasks": len(self._tasks),
                "journal_lines": self._journal_lines,
                "read_only": self.read_only,
            },
        )

    async def stop(self) -> None:
        """Para o varredor, fecha o journal e libera a trava de escrita."""
        if not self._started:
            return
        self._started = False
        if self._reclaim_task is not None:
            self._reclaim_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reclaim_task
            self._reclaim_task = None
        await self._wake_all()
        self._close_journal()
        if self._lock is not None:
            self._lock.release()
            self._lock = None

    async def __aenter__(self) -> Broker:
        """Abre o broker como gerenciador de contexto assíncrono."""
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Fecha o broker ao sair do gerenciador de contexto."""
        await self.stop()

    # ------------------------------------------------------------------ persistência

    def _close_journal(self) -> None:
        """Fecha o handle do journal, se aberto."""
        if self._journal is not None:
            with contextlib.suppress(OSError):
                self._journal.flush()
            with contextlib.suppress(OSError):
                self._journal.close()
            self._journal = None

    def _load_from_disk(self) -> int:
        """Carrega snapshot + journal, devolvendo o total de linhas lidas do journal."""
        self._tasks.clear()
        self._queues.clear()
        self._index.clear()
        offset = self._load_snapshot()
        return offset + self._replay_journal()

    def _load_snapshot(self) -> int:
        """Aplica o snapshot, devolvendo quantas linhas do journal ele já contém."""
        path = self.config.snapshot_path
        offset = 0
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                LOGGER.warning("snapshot ilegível, ignorando: %s", exc)
                payload = {}
            for raw in payload.get("tasks", []) if isinstance(payload, dict) else []:
                try:
                    task = Task.from_dict(raw)
                except (KeyError, TypeError, ValueError, SerializationError) as exc:
                    LOGGER.warning("task inválida no snapshot ignorada: %s", exc)
                    continue
                self._tasks[task.id] = task
                self._track(task)
            offset = int(payload.get("journal_lines", 0)) if isinstance(payload, dict) else 0
        self._journal_offset = offset
        return offset

    def _replay_journal(self) -> int:
        """Aplica as linhas do journal posteriores ao snapshot; devolve quantas linhas leu."""
        path = self.config.journal_path
        if not path.exists():
            return 0
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            LOGGER.warning("journal ilegível: %s", exc)
            return 0
        lines = raw.splitlines()
        self._journal_offset = self._consume(lines, self._journal_offset)
        return len(lines)

    def _consume(self, lines: list[str], inicio: int) -> int:
        """Aplica as linhas de *inicio* em diante e devolve o novo offset consumido.

        Uma última linha inválida **não** é consumida: é o caso de um leitor que
        esbarrou numa gravação ainda pela metade. Ela será lida no próximo refresh.
        """
        offset = inicio
        for index in range(inicio, len(lines)):
            ultima = index == len(lines) - 1
            if not lines[index].strip():
                offset = index + 1
                continue
            if self._apply_line(lines[index], index):
                offset = index + 1
                continue
            if ultima:
                LOGGER.debug("última linha do journal incompleta; aguardando nova leitura")
                break
            LOGGER.warning("linha %d do journal corrompida e ignorada", index + 1)
            offset = index + 1
        return offset

    def _apply_line(self, line: str, index: int) -> bool:
        """Aplica uma linha do journal; devolve ``True`` se houve efeito."""
        line = line.strip()
        if not line:
            return False
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            LOGGER.debug("linha %d do journal não pôde ser lida: %s", index + 1, exc)
            return False
        if not isinstance(record, dict):
            LOGGER.warning("linha %d do journal ignorada (não é um objeto)", index + 1)
            return False
        operation = record.get("op")
        task_id = record.get("id")
        if operation == "remove":
            self._tasks.pop(str(task_id), None)
            self._untrack(str(task_id))
            return True
        raw_task = record.get("task")
        if not isinstance(raw_task, dict) or not raw_task.get("id"):
            return False
        task_id = str(raw_task["id"])
        try:
            existente = self._tasks.get(task_id)
            task = existente.apply_dict(raw_task) if existente is not None else Task.from_dict(raw_task)
        except (KeyError, TypeError, ValueError, SerializationError) as exc:
            LOGGER.warning("linha %d do journal ignorada: %s", index + 1, exc)
            return False
        self._tasks[task_id] = task
        self._track(task)
        return True

    def refresh(self) -> int:
        """Relê o journal a partir da última posição e devolve quantos registros entraram.

        Pensado para consumidores somente-leitura (``monitor``): permite acompanhar
        ao vivo as escritas feitas por outro processo sem tomar a trava de escrita.
        """
        path = self.config.journal_path
        if not path.exists():
            return 0
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            return 0
        lines = raw.splitlines()
        if len(lines) < self._journal_offset:
            # O journal foi truncado por uma compaction: recarrega o snapshot.
            self._load_snapshot()
        anterior = self._journal_offset
        self._journal_offset = self._consume(lines, self._journal_offset)
        return self._journal_offset - anterior

    def _append_record(self, record: dict[str, Any]) -> None:
        """Escreve um registro no journal e compacta se necessário."""
        if self._journal is None:
            return
        self._journal.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._journal.flush()
        if self.config.fsync:
            with contextlib.suppress(OSError):
                os.fsync(self._journal.fileno())
        self._journal_lines += 1
        self._journal_offset = self._journal_lines
        if self.config.compact_lines and self._journal_lines >= self.config.compact_lines:
            self.compact()

    def _append(self, operation: str, task: Task) -> None:
        """Escreve uma linha com a task inteira (last-write-wins por id)."""
        self._append_record(
            {"op": operation, "id": task.id, "ts": time.time(), "task": task.to_dict()}
        )

    def compact(self) -> None:
        """Escreve um snapshot atômico e trunca o journal."""
        if self._journal is None:
            return
        self._prune_ledger()
        tasks = [task.to_dict() for task in sorted(self._tasks.values(), key=lambda item: item.created_at)]
        payload = {
            "version": SNAPSHOT_VERSION,
            "written_at": time.time(),
            "journal_lines": self._journal_lines,
            "tasks": tasks,
        }
        temporary = Path(str(self.config.snapshot_path) + ".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(temporary, self.config.snapshot_path)
        self._close_journal()
        self._journal = self.config.journal_path.open("w", encoding="utf-8", newline="\n")
        self._journal_lines = 0
        self._journal_offset = 0
        LOGGER.info("journal compactado", extra={"tasks": len(tasks)})

    def _prune_ledger(self) -> None:
        """Descarta tasks terminais antigas acima de ``ledger_limit``."""
        limit = self.config.ledger_limit
        if limit <= 0 or len(self._tasks) <= limit:
            return
        terminal = sorted(
            (task for task in self._tasks.values() if task.state.is_terminal),
            key=lambda task: task.finished_at or task.created_at,
        )
        excess = len(self._tasks) - limit
        for task in terminal[:excess]:
            self._tasks.pop(task.id, None)
            self._untrack(task.id)

    @property
    def journal_lines(self) -> int:
        """Quantidade de linhas，目前 conhecidas no journal."""
        return self._journal_lines

    # ------------------------------------------------------------------ índices de fila

    def _get_queue(self, name: str) -> _Queue:
        """Devolve (criando se preciso) a estrutura interna da fila."""
        queue = self._queues.get(name)
        if queue is None:
            queue = _Queue(name=name)
            self._queues[name] = queue
        return queue

    def _track(self, task: Task) -> None:
        """Mantém a heap da fila coerente após carregar ou replayar um registro."""
        if self.read_only:
            return
        if task.state in QUEUEABLE_STATES:
            self._push(task)
        else:
            self._untrack(task.id)

    def _push(self, task: Task) -> None:
        """Insere a task na heap da fila, com nova sequência (FIFO dentro da prioridade).

        Idempotente: se a task já está nessa fila, nada é feito — evita duplicar
        entradas quando o journal é relido por :meth:`refresh`.
        """
        anterior = self._index.get(task.id)
        if anterior == task.queue:
            return
        if anterior is not None:
            self._drop_from_queue(anterior, task.id)
        queue = self._get_queue(task.queue)
        heapq.heappush(queue.heap, (-task.priority, next(self._sequence), task.id))
        self._index[task.id] = task.queue

    def _drop_from_queue(self, queue_name: str, task_id: str) -> None:
        """Remove uma task da heap de uma fila específica (usado em requeue de fila)."""
        queue = self._queues.get(queue_name)
        if queue is None:
            return
        queue.heap = [entry for entry in queue.heap if entry[2] != task_id]
        heapq.heapify(queue.heap)

    def _untrack(self, task_id: str) -> None:
        """Remove a task de qualquer heap em que esteja."""
        queue_name = self._index.pop(task_id, None)
        if queue_name is not None:
            self._drop_from_queue(queue_name, task_id)

    def _claim_locked(self, queue: _Queue, worker_id: str) -> Task | None:
        """Retira da heap a primeira task pronta e a marca como RUNNING com lease."""
        now = time.time()
        while queue.heap:
            _, _, task_id = queue.heap[0]
            task = self._tasks.get(task_id)
            if task is None or task.queue != queue.name or task.state not in QUEUEABLE_STATES:
                heapq.heappop(queue.heap)
                self._index.pop(task_id, None)
                continue
            if task.eta is not None and task.eta > now:
                return None
            heapq.heappop(queue.heap)
            self._index.pop(task_id, None)
            task.set_state(TaskState.RUNNING, strict=True)
            task.attempts += 1
            task.started_at = now
            task.finished_at = None
            task.lease = Lease(
                lease_id=uuid4().hex,
                worker_id=worker_id,
                expires_at=now + self.config.lease_seconds,
            )
            self._append("start", task)
            return task
        return None

    def _eta_wait(self, queue: _Queue) -> float | None:
        """Tempo até a próxima task da fila ficar pronta (retry/cron), limitado a 1s."""
        now = time.time()
        soonest: float | None = None
        for _, _, task_id in queue.heap:
            task = self._tasks.get(task_id)
            if task is None or task.state not in QUEUEABLE_STATES:
                continue
            if task.eta is None:
                return 0.0
            soonest = task.eta if soonest is None else min(soonest, task.eta)
        if soonest is None:
            return None
        return max(0.0, min(soonest - now, _MAX_FETCH_SLICE))

    async def _wake(self, queue_name: str) -> None:
        """Desperta todos os workers esperando na fila informada."""
        queue = self._get_queue(queue_name)
        async with queue.cond:
            queue.cond.notify_all()

    async def _wake_all(self) -> None:
        """Desperta todos os workers de todas as filas."""
        for queue in list(self._queues.values()):
            async with queue.cond:
                queue.cond.notify_all()

    # ------------------------------------------------------------------ API pública

    async def submit(
        self,
        name: str,
        *args: Any,
        queue: str | None = None,
        priority: int | None = None,
        max_retries: int | None = None,
        timeout: float | None = None,
        tags: Mapping[str, str] | None = None,
        delay: float | None = None,
        eta: float | None = None,
        task_id: str | None = None,
        **kwargs: Any,
    ) -> Task:
        """Enfileira uma task registrada.

        Args:
            name: Nome lógico da task (precisa existir no registro).
            *args: Argumentos posicionais da task.
            queue: Fila de destino (padrão: a fila declarada no decorador).
            priority: Prioridade (maior executa antes).
            max_retries: Número de retries (padrão: o do decorador).
            timeout: Timeout em segundos (padrão: o do decorador ou da config).
            tags: Metadados textuais, mesclados com os do decorador.
            delay: Segundos a esperar antes de ficar pronta (mútuo com ``eta``).
            eta: Instante (epoch) em que a task fica pronta.
            task_id: Id explícito (usado por testes e recargas determinísticas).
            **kwargs: Argumentos nomeados da task.

        Returns:
            A :class:`~taskflow.core.task.Task` enfileirada.

        Raises:
            UnknownTaskError: Se o nome não estiver registrado.
            SerializationError: Se os argumentos não forem JSON-safe.
            ReadOnlyBrokerError: Se o broker for somente-leitura.
            BrokerError: Se ``max_retries`` for negativo ou ``delay`` e ``eta`` forem usados juntos.
        """
        self._require_writable("submit")
        registered = self.registry.get(name)
        if delay is not None and eta is not None:
            raise BrokerError("use delay ou eta, não ambos")
        resolved_queue = queue or registered.queue or self.config.default_queue
        resolved_priority = registered.priority if priority is None else int(priority)
        resolved_retries = registered.max_retries if max_retries is None else int(max_retries)
        resolved_timeout = registered.timeout if timeout is None else timeout
        if resolved_timeout is None:
            resolved_timeout = self.config.default_timeout
        if resolved_retries < 0:
            raise BrokerError(f"max_retries não pode ser negativo (recebido {resolved_retries})")
        now = time.time()
        ready_at = eta if eta is not None else (now + delay if delay else None)
        task = Task(
            id=task_id or new_task_id(),
            name=name,
            queue=resolved_queue,
            priority=resolved_priority,
            args=to_jsonable(list(args), path="args"),
            kwargs=to_jsonable(kwargs, path="kwargs"),
            max_retries=resolved_retries,
            timeout=resolved_timeout,
            state=TaskState.PENDING,
            eta=ready_at,
            created_at=now,
            enqueued_at=ready_at,
            tags={**dict(registered.tags), **dict(tags or {})},
        )
        self._tasks[task.id] = task
        self._push(task)
        self._append("enqueue", task)
        await self._wake(task.queue)
        await self.events.publish(
            make_event(
                EventType.ENQUEUED,
                task,
                priority=task.priority,
                max_retries=task.max_retries,
                eta=task.eta,
                tags=dict(task.tags),
            )
        )
        LOGGER.info(
            "task enfileirada",
            extra={
                "task_id": task.short_id,
                "task_name": task.name,
                "queue": task.queue,
                "priority": task.priority,
            },
        )
        return task

    async def fetch(
        self,
        queue: str,
        *,
        worker_id: str = "worker",
        timeout: float | None = None,
    ) -> Task | None:
        """Retira da fila a task de maior prioridade que já esteja pronta.

        Bloqueia até haver trabalho pronto (limitado por *timeout* e pelo ``eta`` das
        tasks em retry). A task devolvida já está em ``RUNNING``, com ``attempts``
        incrementado e uma lease nova.

        Args:
            queue: Fila a consumir.
            worker_id: Identidade do worker, gravada na lease.
            timeout: Espera máxima em segundos (``None`` espera indefinidamente).

        Returns:
            A task em execução ou ``None`` se o timeout estourar.
        """
        self._require_writable("fetch")
        target = self._get_queue(queue)
        deadline = None if timeout is None else time.monotonic() + timeout
        claimed: Task | None = None
        async with target.cond:
            while True:
                claimed = self._claim_locked(target, worker_id)
                if claimed is not None:
                    break
                budget = self._fetch_budget(target, deadline)
                if budget is None:
                    await target.cond.wait()
                    continue
                try:
                    async with asyncio.timeout(budget):
                        await target.cond.wait()
                except TimeoutError:
                    if deadline is not None and time.monotonic() >= deadline:
                        return None
        worker = claimed.lease.worker_id if claimed.lease else worker_id
        await self.events.publish(
            make_event(
                EventType.STARTED,
                claimed,
                worker_id=worker,
                attempt=claimed.attempts,
                lease_id=claimed.lease.lease_id if claimed.lease else None,
                recovered=claimed.recovered,
            )
        )
        LOGGER.info(
            "task iniciada",
            extra={
                "task_id": claimed.short_id,
                "task_name": claimed.name,
                "queue": claimed.queue,
                "attempt": claimed.attempts,
                "worker": worker,
            },
        )
        return claimed

    def _fetch_budget(self, queue: _Queue, deadline: float | None) -> float | None:
        """Calcula quanto tempo o worker pode dormir antes de reavaliar a fila."""
        budget = None if deadline is None else max(0.0, deadline - time.monotonic())
        eta_wait = self._eta_wait(queue)
        if eta_wait is not None:
            budget = eta_wait if budget is None else min(budget, eta_wait)
        return budget

    async def ack(self, task: Task, value: Any = None, *, lease_id: str | None = None) -> Task:
        """Marca a task como ``SUCCESS`` e registra o resultado.

        Args:
            task: Task devolvida por :meth:`fetch`.
            value: Valor devolvido pela função (precisa ser JSON-safe).
            lease_id: Token da lease. Se omitido, usa a lease corrente da task.

        Returns:
            A task atualizada.

        Raises:
            StaleLeaseError: Se a lease apresentada não for a corrente.
            SerializationError: Nunca — resultado não serializável vira falha permanente.
        """
        self._require_writable("ack")
        self._check_lease(task, lease_id)
        try:
            payload = to_jsonable(value)
        except SerializationError as exc:
            return await self.nack(
                task,
                f"resultado não serializável: {exc}",
                permanent=True,
                lease_id=self._resolve_lease_id(task, lease_id),
            )
        now = time.time()
        started = task.started_at or now
        worker_id = task.lease.worker_id if task.lease else None
        task.set_state(TaskState.SUCCESS, strict=True)
        task.finished_at = now
        task.lease = None
        task.eta = None
        task.result = TaskResult(
            task_id=task.id,
            state=TaskState.SUCCESS,
            value=payload,
            attempts=task.attempts,
            worker_id=worker_id,
            started_at=started,
            finished_at=now,
            duration_ms=round((now - started) * 1000, 3),
        )
        self._append("ack", task)
        await self.events.publish(
            make_event(
                EventType.SUCCESS,
                task,
                attempts=task.attempts,
                worker_id=worker_id,
                duration_ms=task.result.duration_ms,
                value=_preview(payload),
            )
        )
        LOGGER.info(
            "task concluída",
            extra={
                "task_id": task.short_id,
                "task_name": task.name,
                "attempts": task.attempts,
                "duration_ms": task.result.duration_ms,
            },
        )
        return task

    async def nack(
        self,
        task: Task,
        error: str,
        *,
        traceback_text: str | None = None,
        retry_delay: float | None = None,
        permanent: bool = False,
        lease_id: str | None = None,
    ) -> Task:
        """Registra uma falha de execução e decide entre retry e dead letter.

        Uma falha vira ``RETRY`` enquanto ``attempts <= max_retries``; ao estourar
        o orçamento ela vira ``DEAD``. Com ``permanent=True`` a task vai direto
        para ``FAILED`` (também na DLQ), usada para falhas que retry não resolve
        (task desconhecida, resultado não serializável, cancelamento).

        Args:
            task: Task devolvida por :meth:`fetch`.
            error: Mensagem de erro.
            traceback_text: Traceback capturado, guardado no resultado.
            retry_delay: Backoff já calculado pelo worker (opcional).
            permanent: Marca a falha como irrecuperável.
            lease_id: Token da lease (fencing).

        Returns:
            A task atualizada.
        """
        self._require_writable("nack")
        self._check_lease(task, lease_id)
        now = time.time()
        started = task.started_at or now
        worker_id = task.lease.worker_id if task.lease else None
        task.last_error = error
        exhausted = task.attempts > task.max_retries
        if permanent or exhausted:
            final_state = TaskState.FAILED if permanent else TaskState.DEAD
            task.set_state(final_state, strict=True)
            task.finished_at = now
            task.lease = None
            task.eta = None
            task.result = TaskResult(
                task_id=task.id,
                state=final_state,
                error=error,
                traceback=traceback_text,
                attempts=task.attempts,
                worker_id=worker_id,
                started_at=started,
                finished_at=now,
                duration_ms=round((now - started) * 1000, 3),
            )
            self._append("nack", task)
            await self.events.publish(
                make_event(
                    EventType.FAILED,
                    task,
                    attempts=task.attempts,
                    worker_id=worker_id,
                    error=error,
                    will_retry=False,
                    permanent=permanent,
                    duration_ms=task.result.duration_ms,
                )
            )
            if final_state is TaskState.DEAD:
                await self.events.publish(
                    make_event(
                        EventType.DEAD,
                        task,
                        attempts=task.attempts,
                        max_retries=task.max_retries,
                        error=error,
                        duration_ms=task.result.duration_ms,
                    )
                )
            LOGGER.warning(
                "task enviada para a dead letter queue",
                extra={
                    "task_id": task.short_id,
                    "task_name": task.name,
                    "state": final_state.value,
                    "attempts": task.attempts,
                    "max_retries": task.max_retries,
                    "error": error,
                },
            )
            return task

        delay = retry_delay if retry_delay is not None else self._default_delay(task)
        delay = max(0.0, float(delay))
        task.set_state(TaskState.RETRY, strict=True)
        task.finished_at = now
        task.started_at = None
        task.lease = None
        task.eta = now + delay
        self._push(task)
        self._append("nack", task)
        await self._wake(task.queue)
        await self.events.publish(
            make_event(
                EventType.FAILED,
                task,
                attempts=task.attempts,
                max_retries=task.max_retries,
                worker_id=worker_id,
                error=error,
                will_retry=True,
                retry_in=round(delay, 3),
                next_eta=task.eta,
            )
        )
        LOGGER.warning(
            "task agendada para retry",
            extra={
                "task_id": task.short_id,
                "task_name": task.name,
                "attempt": task.attempts,
                "max_retries": task.max_retries,
                "retry_in": round(delay, 3),
                "error": error,
            },
        )
        return task

    def _default_delay(self, task: Task) -> float:
        """Backoff padrão quando o worker não informa um valor."""
        from taskflow.worker.retry import RetryPolicy  # import tardio: worker→core, evita ciclo

        policy = RetryPolicy(
            base=self.config.retry_base,
            cap=self.config.retry_cap,
            jitter=self.config.retry_jitter,
        )
        return policy.delay_for(task.attempts)

    def renew_lease(self, task: Task, lease_id: str, *, lease_seconds: float | None = None) -> bool:
        """Prorroga a lease de uma task em execução (heartbeat do worker).

        Args:
            task: Task em execução.
            lease_id: Token da lease que se quer renovar.
            lease_seconds: Nova duração; padrão: ``config.lease_seconds``.

        Returns:
            ``True`` se a lease foi renovada, ``False`` se já não é a corrente.
        """
        if self.read_only:
            return False
        stored = self._tasks.get(task.id)
        if stored is None or stored.state is not TaskState.RUNNING or stored.lease is None:
            return False
        if stored.lease.lease_id != lease_id:
            return False
        stored.lease.expires_at = time.time() + (lease_seconds or self.config.lease_seconds)
        self._append("renew", stored)
        return True

    async def reclaim_expired(self, now: float | None = None) -> list[Task]:
        """Devolve à fila as tasks cujo worker desapareceu (lease vencida).

        Args:
            now: Instante de referência (padrão: agora).

        Returns:
            As tasks devolvidas à fila.
        """
        if self.read_only:
            return []
        moment = time.time() if now is None else now
        reclaimed: list[Task] = []
        for task in list(self._tasks.values()):
            if task.state is not TaskState.RUNNING or task.lease is None:
                continue
            if task.lease.expires_at > moment:
                continue
            self._requeue_abandoned(task, moment)
            reclaimed.append(task)
        if reclaimed:
            await self._wake_all()
            for task in reclaimed:
                await self.events.publish(
                    make_event(
                        EventType.ENQUEUED,
                        task,
                        recovered=True,
                        attempts=task.attempts,
                        recovered_count=task.recovered,
                    )
                )
            LOGGER.warning(
                "tasks abandonadas devolvidas à fila",
                extra={"quantidade": len(reclaimed)},
            )
        return reclaimed

    def _requeue_abandoned(self, task: Task, now: float) -> None:
        """Devolve uma task em execução cuja lease venceu para o estado pendente."""
        previous_worker = task.lease.worker_id if task.lease else None
        task.set_state(TaskState.PENDING, strict=True)
        task.lease = None
        task.started_at = None
        task.finished_at = None
        task.recovered += 1
        task.eta = now
        self._push(task)
        self._append("reclaim", task)
        LOGGER.warning(
            "lease expirada, task reconvertida para pendente",
            extra={
                "task_id": task.short_id,
                "task_name": task.name,
                "attempts": task.attempts,
                "worker_anterior": previous_worker,
            },
        )

    def _recover_running_tasks(self) -> None:
        """No start, devolve ao estado pendente as tasks que estavam em execução.

        Válido porque a trava de escrita garante um único processo escritor por
        ``data_dir``: se ninguém está executando, o worker anterior morreu.
        """
        now = time.time()
        for task in list(self._tasks.values()):
            if task.state is TaskState.RUNNING:
                self._requeue_abandoned(task, now)

    async def _reclaim_loop(self) -> None:
        """Varre periodicamente as leases vencidas."""
        while True:
            await asyncio.sleep(self.config.reclaim_interval)
            try:
                await self.reclaim_expired()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("falha ao varrer leases expiradas")

    async def requeue(
        self,
        task_id: str,
        *,
        queue: str | None = None,
        priority: int | None = None,
        reset_attempts: bool = True,
        delay: float | None = None,
    ) -> Task:
        """Reenfileira uma task terminal (requeue da DLQ).

        Args:
            task_id: Id da task.
            queue: Nova fila (padrão: a fila original).
            priority: Nova prioridade.
            reset_attempts: Zera ``attempts`` para dar um orçamento novo de retries.
            delay: Segundos até a task ficar pronta.

        Returns:
            A task reenfileirada.

        Raises:
            TaskNotFoundError: Se a task não existir.
            BrokerError: Se a task ainda não for terminal.
        """
        self._require_writable("requeue")
        task = self.require(task_id)
        if not task.state.is_terminal:
            raise BrokerError(
                f"só é possível requeuear tasks terminais; {task.short_id} está em {task.state.value}"
            )
        now = time.time()
        previous_queue = task.queue
        previous_state = task.state.value
        task.set_state(TaskState.PENDING, strict=True)
        if reset_attempts:
            task.attempts = 0
        if queue:
            task.queue = queue
        if priority is not None:
            task.priority = int(priority)
        task.lease = None
        task.result = None
        task.last_error = None
        task.started_at = None
        task.finished_at = None
        task.eta = now + delay if delay else now
        task.enqueued_at = task.eta
        if previous_queue != task.queue:
            self._drop_from_queue(previous_queue, task.id)
        self._push(task)
        self._append("requeue", task)
        await self._wake(task.queue)
        await self.events.publish(
            make_event(
                EventType.ENQUEUED,
                task,
                requeued=True,
                previous_state=previous_state,
                priority=task.priority,
                attempts=task.attempts,
            )
        )
        LOGGER.info(
            "task reenfileirada",
            extra={
                "task_id": task.short_id,
                "task_name": task.name,
                "queue": task.queue,
                "attempts": task.attempts,
            },
        )
        return task

    async def remove(self, task_id: str) -> bool:
        """Remove uma task do ledger (purge). Devolve ``True`` se ela existia."""
        self._require_writable("remove")
        if task_id not in self._tasks:
            return False
        self._untrack(task_id)
        self._tasks.pop(task_id, None)
        self._append_record({"op": "remove", "id": task_id, "ts": time.time()})
        return True

    # ------------------------------------------------------------------ consultas

    def get(self, task_id: str) -> Task | None:
        """Devolve a task pelo id ou ``None``."""
        return self._tasks.get(task_id)

    def require(self, task_id: str) -> Task:
        """Devolve a task pelo id.

        Raises:
            TaskNotFoundError: Se o id não existir.
        """
        task = self._tasks.get(task_id)
        if task is None:
            raise TaskNotFoundError(f"task não encontrada: {task_id}")
        return task

    def list_tasks(self, task_filter: TaskFilter | None = None) -> list[Task]:
        """Lista as tasks do ledger aplicando filtro e ordenação."""
        criteria = task_filter or TaskFilter()
        selected = [task for task in self._tasks.values() if criteria.matches(task)]
        selected.sort(key=criteria.sort_key)
        if criteria.limit and criteria.limit > 0:
            return selected[: criteria.limit]
        return selected

    def counts(self) -> dict[str, int]:
        """Contagem de tasks por estado."""
        counter = {state.value: 0 for state in TaskState}
        for task in self._tasks.values():
            counter[task.state.value] += 1
        return counter

    def ready_count(self) -> int:
        """Quantas tasks estão na fila e prontas para execução agora."""
        return sum(1 for task in self._tasks.values() if task.is_ready)

    def queued_count(self) -> int:
        """Quantas tasks estão na fila, inclusive as que esperam o ``eta`` de um retry."""
        return sum(1 for task in self._tasks.values() if task.state in QUEUEABLE_STATES)

    def has_pending_work(self) -> bool:
        """``True`` se há task na fila (mesmo aguardando retry) ou task em execução.

        Usado pelo ``drain``: uma task em ``RETRY`` continua sendo trabalho a fazer.
        """
        if self.queued_count() > 0:
            return True
        return any(task.state is TaskState.RUNNING for task in self._tasks.values())

    def queue_names(self) -> list[str]:
        """Nomes de filas conhecidos (configuração + filas já usadas)."""
        names = set(self.config.queues) | set(self._queues)
        names.update(task.queue for task in self._tasks.values())
        return sorted(names)

    def queue_stats(self) -> tuple[QueueStats, ...]:
        """Contagem por fila e estado."""
        buckets: dict[str, dict[str, int]] = {
            name: {state.value: 0 for state in TaskState} for name in self.queue_names()
        }
        for task in self._tasks.values():
            buckets.setdefault(task.queue, {state.value: 0 for state in TaskState})
            buckets[task.queue][task.state.value] += 1
        return tuple(
            QueueStats(
                queue=name,
                pending=buckets[name][TaskState.PENDING.value],
                retrying=buckets[name][TaskState.RETRY.value],
                running=buckets[name][TaskState.RUNNING.value],
                succeeded=buckets[name][TaskState.SUCCESS.value],
                failed=buckets[name][TaskState.FAILED.value],
                dead=buckets[name][TaskState.DEAD.value],
            )
            for name in sorted(buckets)
        )

    def stats(self) -> BrokerStats:
        """Fotografia agregada do broker."""
        counter = self.counts()
        return BrokerStats(
            queues=self.queue_stats(),
            total=len(self._tasks),
            ready=self.ready_count(),
            queued=self.queued_count(),
            in_flight=counter[TaskState.RUNNING.value],
            dead_lettered=counter[TaskState.DEAD.value] + counter[TaskState.FAILED.value],
            succeeded=counter[TaskState.SUCCESS.value],
            failed=counter[TaskState.FAILED.value],
            started_at=self._started_at,
            uptime_seconds=max(0.0, time.time() - self._started_at),
            journal_lines=self._journal_lines,
        )

    # ------------------------------------------------------------------ internos

    def _require_writable(self, operation: str) -> None:
        """Levanta erro se o broker for somente-leitura."""
        if self.read_only:
            raise ReadOnlyBrokerError(
                f"broker aberto somente para leitura; a operação {operation!r} exige escrita "
                "(rode um comando que escreva, por exemplo 'taskflow.cli submit' ou 'worker')"
            )

    def _resolve_lease_id(self, task: Task, lease_id: str | None) -> str:
        """Devolve o token da lease informada ou, na falta, o token corrente."""
        if lease_id:
            return lease_id
        return task.lease.lease_id if task.lease else ""

    def _check_lease(self, task: Task, lease_id: str | None) -> None:
        """Valida o fencing token antes de gravar o resultado de uma execução.

        Raises:
            StaleLeaseError: Se a task não estiver mais em execução com essa lease.
        """
        stored = self._tasks.get(task.id)
        if stored is None:
            raise TaskNotFoundError(f"task não encontrada: {task.id}")
        expected = self._resolve_lease_id(task, lease_id)
        current = stored.lease.lease_id if stored.lease else None
        if current is None:
            raise StaleLeaseError(
                f"a task {task.short_id} não está mais em execução "
                f"(estado {stored.state.value}); o resultado foi descartado"
            )
        if current != expected:
            raise StaleLeaseError(
                f"lease obsoleta na task {task.short_id}: esperada {current[:8]}, "
                f"recebida {expected[:8]}"
            )