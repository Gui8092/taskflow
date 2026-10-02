"""Event bus assíncrono com histórico, usado por workers, CLI e dashboard.

O broker publica os eventos do ciclo de vida; consumidores (monitor, dashboard,
testes) se inscrevem com :meth:`EventBus.subscribe`. A publicação é **sequencial**
na ordem de inscrição, o que torna a ordem dos eventos observável e determinística.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Final

from taskflow.core.task import Task

LOGGER: Final[logging.Logger] = logging.getLogger("taskflow.events")


class EventType(str, Enum):
    """Eventos emitidos pelo broker durante o ciclo de vida de uma task."""

    ENQUEUED = "enqueued"
    STARTED = "started"
    SUCCESS = "success"
    FAILED = "failed"
    DEAD = "dead"
    CANCELLED = "cancelled"

    def __str__(self) -> str:
        """Devolve o valor textual do evento."""
        return self.value

    @classmethod
    def coerce(cls, value: EventType | str) -> EventType:
        """Converte texto em :class:`EventType`, aceitando maiúsculas/minúsculas."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as exc:
            raise ValueError(f"evento desconhecido: {value!r}") from exc


@dataclass(slots=True)
class Event:
    """Ocorrência observável no ciclo de vida de uma task."""

    type: EventType
    task_id: str
    queue: str
    name: str
    timestamp: float
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Converte o evento para um dicionário JSON-safe (formato do WebSocket)."""
        return {
            "type": self.type.value,
            "task_id": self.task_id,
            "queue": self.queue,
            "name": self.name,
            "timestamp": self.timestamp,
            "payload": dict(self.payload),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Event:
        """Reconstrói um evento a partir do dicionário serializado."""
        return cls(
            type=EventType.coerce(data["type"]),
            task_id=str(data["task_id"]),
            queue=str(data.get("queue", "default")),
            name=str(data.get("name", "")),
            timestamp=float(data.get("timestamp", 0.0)),
            payload=dict(data.get("payload") or {}),
        )


def make_event(event_type: EventType, task: Task, **payload: Any) -> Event:
    """Cria um evento preenchendo os campos comuns a partir de uma :class:`Task`."""
    return Event(
        type=event_type,
        task_id=task.id,
        queue=task.queue,
        name=task.name,
        timestamp=time.time(),
        payload=payload,
    )


#: Assinatura aceita para callbacks de assinantes (síncronos ou assíncronos).
Subscriber = Callable[[Event], "Awaitable[None] | None"]


@dataclass(slots=True)
class Subscription:
    """Inscrição de um callback no :class:`EventBus`."""

    callback: Subscriber
    event_type: EventType | None = None
    name: str = ""
    _bus: EventBus | None = field(default=None, repr=False, compare=False)

    def matches(self, event: Event) -> bool:
        """Diz se a inscrição quer receber o evento."""
        return self.event_type is None or self.event_type is event.type

    def unsubscribe(self) -> None:
        """Desfaz a inscrição no bus que a criou."""
        if self._bus is not None:
            self._bus.unsubscribe(self)


class EventBus:
    """Barramento de eventos com publicação sequencial e histórico circular."""

    def __init__(self, history: int = 500) -> None:
        """Cria o bus guardando até *history* eventos recentes."""
        self._subscriptions: list[Subscription] = []
        self._history: deque[Event] = deque(maxlen=history)
        self._signal = asyncio.Event()

    @property
    def subscriber_count(self) -> int:
        """Quantidade de inscrições ativas."""
        return len(self._subscriptions)

    def subscribe(
        self,
        callback: Subscriber,
        *,
        event_type: EventType | None = None,
        name: str = "",
    ) -> Subscription:
        """Registra *callback* para receber eventos.

        Args:
            callback: Função que recebe um :class:`Event`; pode ser ``def`` ou ``async def``.
            event_type: Filtro opcional por tipo de evento.
            name: Nome descritivo (aparece nos logs).

        Returns:
            A :class:`Subscription` criada, que pode ser cancelada pelo chamador.
        """
        subscription = Subscription(callback=callback, event_type=event_type, name=name)
        subscription._bus = self  # noqa: SLF001 — ligação interna intencional
        self._subscriptions.append(subscription)
        return subscription

    def unsubscribe(self, subscription: Subscription | Subscriber) -> None:
        """Remove uma inscrição (pelo objeto ou pelo callback)."""
        target = subscription.callback if isinstance(subscription, Subscription) else subscription
        self._subscriptions = [item for item in self._subscriptions if item.callback is not target]

    async def publish(self, event: Event) -> None:
        """Entrega o evento a todos os assinantes, na ordem de inscrição.

        Exceções dos assinantes são registradas em log e não interrompem a
        entrega nem o fluxo do broker.
        """
        self._history.append(event)
        self._signal.set()
        for subscription in list(self._subscriptions):
            if not subscription.matches(event):
                continue
            try:
                outcome = subscription.callback(event)
                if inspect.isawaitable(outcome):
                    await outcome
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception(
                    "assinante de evento falhou",
                    extra={"event": event.type.value, "task_id": event.task_id, "subscriber": subscription.name},
                )

    def history(
        self,
        limit: int | None = None,
        *,
        event_type: EventType | None = None,
        task_id: str | None = None,
    ) -> list[Event]:
        """Devolve os eventos recentes, do mais antigo para o mais novo."""
        events = list(self._history)
        if event_type is not None:
            events = [event for event in events if event.type is event_type]
        if task_id is not None:
            events = [event for event in events if event.task_id == task_id]
        if limit is not None and limit >= 0:
            events = events[-limit:]
        return events

    async def wait_for(
        self,
        predicate: Callable[[Event], bool],
        *,
        timeout: float = 1.0,
        poll: float = 0.005,
    ) -> Event | None:
        """Aguarda um evento que satisfaça *predicate*.

        Args:
            predicate: Função que decide se o evento interessa.
            timeout: Tempo máximo de espera em segundos.
            poll: Intervalo de reavaliação quando não há sinal novo.

        Returns:
            O primeiro evento que satisfaz o predicado ou ``None`` no timeout.
        """
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            self._signal.clear()
            for event in reversed(self._history):
                if predicate(event):
                    return event
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                async with asyncio.timeout(min(poll, remaining)):
                    await self._signal.wait()
            except TimeoutError:
                continue