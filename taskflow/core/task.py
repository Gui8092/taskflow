"""Estruturas de dados de uma task: :class:`Task`, :class:`TaskResult` e :class:`Lease`.

Uma :class:`Task` é a unidade persistida pelo broker. Ela carrega os argumentos
da chamada, a prioridade, o orçamento de retries, os *timestamps* do ciclo de
vida e, enquanto executa, a :class:`Lease` que impede execução duplicada.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field, fields
from typing import Any, Final, Mapping
from uuid import uuid4

from taskflow.core.states import TaskState, validate_transition
from taskflow.core.serialization import SerializationError, to_jsonable

#: Estados em que a task ocupa espaço na fila e pode ser buscada por um worker.
QUEUEABLE_STATES: Final[frozenset[TaskState]] = frozenset({TaskState.PENDING, TaskState.RETRY})


def new_task_id() -> str:
    """Gera um identificador único (uuid4 hexadecimal) para uma nova task."""
    return uuid4().hex


@dataclass(slots=True)
class Lease:
    """Direito de execução temporário sobre uma task.

    O :attr:`lease_id` funciona como *fencing token*: ``ack``/``nack`` só são
    aceitos se o token da task ainda for o mesmo que o emitido no ``fetch``.
    """

    lease_id: str
    worker_id: str
    expires_at: float

    def is_expired(self, now: float | None = None) -> bool:
        """Diz se a lease já venceu."""
        return (time.time() if now is None else now) >= self.expires_at

    def to_dict(self) -> dict[str, Any]:
        """Converte a lease para um dicionário JSON-safe."""
        return {
            "lease_id": self.lease_id,
            "worker_id": self.worker_id,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Lease:
        """Reconstrói uma lease a partir do dicionário persistido."""
        return cls(
            lease_id=str(data["lease_id"]),
            worker_id=str(data.get("worker_id", "")),
            expires_at=float(data["expires_at"]),
        )


@dataclass(slots=True)
class TaskResult:
    """Resultado de uma execução: valor devolvido ou erro capturado."""

    task_id: str
    state: TaskState
    value: Any = None
    error: str | None = None
    traceback: str | None = None
    attempts: int = 0
    worker_id: str | None = None
    started_at: float | None = None
    finished_at: float | None = None
    duration_ms: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Converte o resultado para um dicionário JSON-safe."""
        return {
            "task_id": self.task_id,
            "state": self.state.value,
            "value": self.value,
            "error": self.error,
            "traceback": self.traceback,
            "attempts": self.attempts,
            "worker_id": self.worker_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> TaskResult | None:
        """Reconstrói um resultado a partir do dicionário persistido (``None`` mantém ``None``)."""
        if not data:
            return None
        return cls(
            task_id=str(data["task_id"]),
            state=TaskState.coerce(data["state"]),
            value=data.get("value"),
            error=data.get("error"),
            traceback=data.get("traceback"),
            attempts=int(data.get("attempts", 0)),
            worker_id=data.get("worker_id"),
            started_at=_opt_float(data.get("started_at")),
            finished_at=_opt_float(data.get("finished_at")),
            duration_ms=_opt_float(data.get("duration_ms")),
        )


def _opt_float(value: Any) -> float | None:
    """Converte um valor arbitrário em ``float | None``."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass(slots=True)
class Task:
    """Uma unidade de trabalho persistida pelo broker."""

    id: str
    name: str
    queue: str
    priority: int = 0
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    attempts: int = 0
    max_retries: int = 3
    timeout: float | None = None
    state: TaskState = TaskState.PENDING
    eta: float | None = None
    created_at: float = field(default_factory=time.time)
    enqueued_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    last_error: str | None = None
    recovered: int = 0
    result: TaskResult | None = None
    lease: Lease | None = None
    tags: dict[str, str] = field(default_factory=dict)

    @property
    def is_ready(self) -> bool:
        """``True`` quando a task está na fila e já pode ser buscada (``eta`` vencido)."""
        if self.state not in QUEUEABLE_STATES:
            return False
        return self.eta is None or self.eta <= time.time()

    @property
    def duration_ms(self) -> float | None:
        """Duração da última execução em milissegundos, quando conhecida."""
        if self.started_at is None:
            return None
        end = self.finished_at if self.finished_at is not None else time.time()
        return round((end - self.started_at) * 1000, 3)

    @property
    def short_id(self) -> str:
        """Prefixo do id, usado nas telas da CLI e do dashboard."""
        return self.id[:8]

    def transition_to(self, state: TaskState | str) -> None:
        """Muda o estado validando a transição.

        Raises:
            StateError: Se a transição não for permitida pelo ciclo de vida.
        """
        target = TaskState.coerce(state)
        validate_transition(self.state, target)
        self.state = target

    def set_state(self, state: TaskState | str, *, strict: bool = False) -> None:
        """Muda o estado; com ``strict=True`` valida a transição.

        A validação é desligada por padrão porque a recuperação do journal pode
        reordenar escritas (por exemplo, um ``ack`` gravado antes de um ``nack``
        em outro processo).
        """
        if strict:
            self.transition_to(state)
        else:
            self.state = TaskState.coerce(state)

    def to_dict(self) -> dict[str, Any]:
        """Converte a task para um dicionário JSON-safe (é o que vai para o journal)."""
        return {
            "id": self.id,
            "name": self.name,
            "queue": self.queue,
            "priority": self.priority,
            "args": to_jsonable(self.args),
            "kwargs": to_jsonable(self.kwargs),
            "attempts": self.attempts,
            "max_retries": self.max_retries,
            "timeout": self.timeout,
            "state": self.state.value,
            "eta": self.eta,
            "created_at": self.created_at,
            "enqueued_at": self.enqueued_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "last_error": self.last_error,
            "recovered": self.recovered,
            "result": self.result.to_dict() if self.result else None,
            "lease": self.lease.to_dict() if self.lease else None,
            "tags": dict(self.tags),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Task:
        """Reconstrói uma task a partir do dicionário persistido.

        Raises:
            KeyError: Se faltar algum campo obrigatório (``id``, ``name`` ou ``queue``).
            SerializationError: Se algum campo tiver tipo inesperado.
        """
        try:
            args = data.get("args", [])
            kwargs = data.get("kwargs", {})
            if not isinstance(args, list) or not isinstance(kwargs, dict):
                raise SerializationError("args deve ser lista e kwargs deve ser objeto")
            task = cls(
                id=str(data["id"]),
                name=str(data["name"]),
                queue=str(data["queue"]),
                priority=int(data.get("priority", 0)),
                args=list(args),
                kwargs=dict(kwargs),
                attempts=int(data.get("attempts", 0)),
                max_retries=int(data.get("max_retries", 3)),
                timeout=_opt_float(data.get("timeout")),
                state=TaskState.coerce(data.get("state", TaskState.PENDING)),
                eta=_opt_float(data.get("eta")),
                created_at=float(data.get("created_at", 0.0)),
                enqueued_at=_opt_float(data.get("enqueued_at")),
                started_at=_opt_float(data.get("started_at")),
                finished_at=_opt_float(data.get("finished_at")),
                last_error=data.get("last_error"),
                recovered=int(data.get("recovered", 0)),
                result=TaskResult.from_dict(data.get("result")),
                lease=Lease.from_dict(data["lease"]) if data.get("lease") else None,
                tags=dict(data.get("tags") or {}),
            )
        except KeyError as exc:
            raise KeyError(f"campo obrigatório ausente na task persistida: {exc.args[0]}") from exc
        except (TypeError, ValueError) as exc:
            raise SerializationError(f"task persistida inválida: {exc}") from exc
        return task

    def apply_dict(self, data: Mapping[str, Any]) -> Task:
        """Atualiza esta task a partir de um dicionário persistido, mantendo o objeto.

        Usado pelo broker ao reler o journal: como a mesma instância continua viva,
        workers que já detêm a task (com a lease em mãos) seguem vendo o estado novo.

        Args:
            data: Dicionário no mesmo formato de :meth:`from_dict`.

        Returns:
            A própria task, já atualizada.

        Raises:
            KeyError: Se faltar algum campo obrigatório.
            SerializationError: Se algum campo tiver tipo inesperado.
        """
        atualizado = Task.from_dict(data)
        for campo in fields(self):
            setattr(self, campo.name, getattr(atualizado, campo.name))
        return self

    def summary(self) -> dict[str, Any]:
        """Devolve um resumo compacto para a CLI e para o dashboard."""
        return {
            "id": self.id,
            "name": self.name,
            "queue": self.queue,
            "state": self.state.value,
            "priority": self.priority,
            "attempts": self.attempts,
            "max_retries": self.max_retries,
            "recovered": self.recovered,
            "duration_ms": self.duration_ms,
            "created_at": self.created_at,
            "enqueued_at": self.enqueued_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "eta": self.eta,
            "timeout": self.timeout,
            "last_error": self.last_error,
            "worker_id": (
                self.lease.worker_id
                if self.lease
                else (self.result.worker_id if self.result else None)
            ),
            "tags": dict(self.tags),
            "result": self.result.to_dict() if self.result else None,
        }


@dataclass(frozen=True, slots=True)
class TaskFilter:
    """Critérios de seleção e ordenação para :meth:`taskflow.core.broker.Broker.list_tasks`."""

    state: TaskState | None = None
    queue: str | None = None
    name: str | None = None
    limit: int = 100
    newest_first: bool = True

    def matches(self, task: Task) -> bool:
        """Diz se a task satisfaz todos os critérios do filtro."""
        if self.state is not None and task.state is not self.state:
            return False
        if self.queue is not None and task.queue != self.queue:
            return False
        if self.name is not None and task.name != self.name:
            return False
        return True

    def sort_key(self, task: Task) -> tuple[float, float, int]:
        """Chave de ordenação: timestamp de referência, prioridade e id (desempate estável)."""
        reference = task.finished_at or task.started_at or task.enqueued_at or task.created_at
        return (-reference if self.newest_first else reference, -task.priority, task.id)