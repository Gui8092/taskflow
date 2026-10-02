"""Estados possíveis de uma task e as transições válidas entre eles.

O ciclo de vida implementado pelo broker é::

    PENDING ──▶ RUNNING ──▶ SUCCESS
       ▲          │  ▲
       │          │  └──▶ RETRY ──▶ (nova tentativa) ──▶ RUNNING
       │          │
       └── reclaim └──▶ FAILED  (falha permanente, sem retry possível)
                     └──▶ DEAD    (esgotou max_retries; mora na DLQ)

``FAILED`` e ``DEAD`` são ambos terminais e vivem na dead letter queue: o
primeiro representa falha permanente detectada na hora (task desconhecida,
resultado não serializável, cancelamento) e o segundo representa esgotamento
do orçamento de retries.
"""

from __future__ import annotations

from enum import Enum
from typing import Final, Mapping


class TaskState(str, Enum):
    """Estado de uma task no broker."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRY = "RETRY"
    DEAD = "DEAD"

    def __str__(self) -> str:
        """Devolve o valor textual do estado (útil em logs e na UI)."""
        return self.value

    @property
    def is_terminal(self) -> bool:
        """``True`` quando não há mais transições possíveis a partir deste estado."""
        return self in TERMINAL_STATES

    @property
    def is_pending(self) -> bool:
        """``True`` para estados em que a task ainda não começou a executar nesta tentativa."""
        return self in PENDING_STATES

    @property
    def is_failure(self) -> bool:
        """``True`` para os estados que representam falha."""
        return self in FAILURE_STATES

    @classmethod
    def coerce(cls, value: TaskState | str) -> TaskState:
        """Converte texto em :class:`TaskState`, aceitando maiúsculas/minúsculas."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().upper())
        except ValueError as exc:
            raise StateError(
                f"estado desconhecido: {value!r} (válidos: {', '.join(state.value for state in cls)})"
            ) from exc


#: Estados em que a task está na fila mas ainda não foi executada nesta tentativa.
PENDING_STATES: Final[frozenset[TaskState]] = frozenset({TaskState.PENDING, TaskState.RETRY})

#: Estados finais: a task não volta mais para a fila por conta própria.
TERMINAL_STATES: Final[frozenset[TaskState]] = frozenset(
    {TaskState.SUCCESS, TaskState.FAILED, TaskState.DEAD}
)

#: Estados de falha (todos terminais e presentes na dead letter queue).
FAILURE_STATES: Final[frozenset[TaskState]] = frozenset({TaskState.FAILED, TaskState.DEAD})

#: Estados em que a task ainda pode ser executada.
ACTIVE_STATES: Final[frozenset[TaskState]] = PENDING_STATES | {TaskState.RUNNING}

#: Transições aceitas pelo broker. Estados terminais não possuem saída, exceto o
#: requeue explícito da DLQ (``FAILED``/``DEAD`` → ``PENDING``).
VALID_TRANSITIONS: Final[Mapping[TaskState, frozenset[TaskState]]] = {
    TaskState.PENDING: frozenset(
        {TaskState.RUNNING, TaskState.RETRY, TaskState.FAILED, TaskState.DEAD}
    ),
    TaskState.RUNNING: frozenset(
        {
            TaskState.SUCCESS,
            TaskState.RETRY,
            TaskState.FAILED,
            TaskState.DEAD,
            TaskState.PENDING,
        }
    ),
    TaskState.RETRY: frozenset(
        {TaskState.RUNNING, TaskState.FAILED, TaskState.DEAD, TaskState.PENDING}
    ),
    TaskState.SUCCESS: frozenset(),
    TaskState.FAILED: frozenset({TaskState.PENDING}),
    TaskState.DEAD: frozenset({TaskState.PENDING}),
}

_LABELS: Final[Mapping[TaskState, str]] = {
    TaskState.PENDING: "pendente",
    TaskState.RUNNING: "executando",
    TaskState.SUCCESS: "sucesso",
    TaskState.FAILED: "falha permanente",
    TaskState.RETRY: "aguardando retry",
    TaskState.DEAD: "dead letter",
}


class StateError(RuntimeError):
    """Transição de estado inválida para o ciclo de vida da task."""


def can_transition(current: TaskState, target: TaskState) -> bool:
    """Diz se a transição ``current → target`` é permitida."""
    return target in VALID_TRANSITIONS[TaskState.coerce(current)]


def validate_transition(current: TaskState, target: TaskState) -> None:
    """Valida a transição ``current → target``.

    Raises:
        StateError: Se a transição não estiver em :data:`VALID_TRANSITIONS`.
    """
    origin = TaskState.coerce(current)
    destination = TaskState.coerce(target)
    if destination not in VALID_TRANSITIONS[origin]:
        allowed = ", ".join(sorted(state.value for state in VALID_TRANSITIONS[origin])) or "nenhuma"
        raise StateError(f"transição inválida: {origin.value} → {destination.value} (permitido: {allowed})")


def is_terminal(state: TaskState | str) -> bool:
    """Diz se o estado é final."""
    return TaskState.coerce(state).is_terminal


def describe_state(state: TaskState | str) -> str:
    """Devolve um rótulo curto em português para uso na CLI e no dashboard."""
    return _LABELS[TaskState.coerce(state)]