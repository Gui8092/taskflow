"""Agendador cron: enfileira tasks periódicas a partir de expressões cron.

O loop do :class:`Scheduler` funciona em fatias curtas (``max_sleep``), para
responder a :meth:`Scheduler.stop` rapidamente, e calcula a próxima execução de
cada schedule a partir do instante atual — **sem tempestade de catch-up**: se o
processo ficou 10 minutos parado, ele dispara uma vez, não 10.

``clock`` e ``sleep`` são injetáveis, o que permite testar o agendamento sem
espera real.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Final, Mapping, Sequence

from taskflow.core.broker import Broker
from taskflow.core.config import Config
from taskflow.core.registry import RegisteredTask
from taskflow.core.serialization import to_jsonable
from taskflow.scheduler.cron import CronError, CronExpression

LOGGER: Final[logging.Logger] = logging.getLogger("taskflow.scheduler")

#: Fatia máxima de sono do loop (segundos).
DEFAULT_MAX_SLEEP: Final[float] = 1.0


class SchedulerError(RuntimeError):
    """Erro de configuração do agendador."""


def format_seconds(value: float) -> str:
    """Formata segundos de forma legível: ``45`` vira ``"45"``, ``0.05`` vira ``"0.05"``."""
    return f"{float(value):g}"


@dataclass(slots=True)
class Schedule:
    """Uma task periódica, disparada por expressão cron **ou** por intervalo fixo.

    Exatamente um dos dois modos precisa ser informado: ``cron`` (granularidade de
    1 minuto, com a semântica do ``crontab``) ou ``interval`` em segundos, que
    permite disparos mais frequentes que um minuto.
    """

    name: str
    cron: CronExpression | None = None
    interval: float | None = None
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    queue: str | None = None
    priority: int | None = None
    max_retries: int | None = None
    tags: dict[str, str] = field(default_factory=dict)
    next_run_at: float | None = None
    last_run_at: float | None = None
    last_task_id: str | None = None
    run_count: int = 0
    error_count: int = 0

    def __post_init__(self) -> None:
        """Valida que exatamente um modo de disparo foi informado."""
        if (self.cron is None) == (self.interval is None):
            raise SchedulerError(
                f"schedule {self.name!r}: informe exatamente um modo de disparo "
                "(cron ou interval)"
            )
        if self.interval is not None and self.interval <= 0:
            raise SchedulerError(
                f"schedule {self.name!r}: interval precisa ser > 0 (recebido {self.interval})"
            )

    @property
    def expression(self) -> str:
        """Descrição do gatilho, como escrito na configuração."""
        if self.cron is not None:
            return self.cron.expression
        return f"a cada {format_seconds(self.interval or 0.0)}s"

    def compute_next(self, now: float) -> float:
        """Calcula o instante (epoch) da próxima execução a partir de *now*.

        Para intervalo, o prazo é contado a partir da última execução — mas nunca
        acumula atraso: se o processo ficou parado, o próximo disparo é ``now +
        interval``, não uma rajada de execuções perdidas.
        """
        if self.cron is not None:
            return self.cron.next_after(datetime.fromtimestamp(now)).timestamp()
        base = self.last_run_at if self.last_run_at is not None else now
        proximo = base + float(self.interval or 0.0)
        if proximo <= now:
            proximo = now + float(self.interval or 0.0)
        return proximo

    def to_dict(self) -> dict[str, Any]:
        """Converte o schedule para um dicionário JSON-safe."""
        return {
            "name": self.name,
            "cron": self.cron.expression if self.cron is not None else None,
            "interval": self.interval,
            "expression": self.expression,
            "queue": self.queue,
            "priority": self.priority,
            "next_run_at": self.next_run_at,
            "last_run_at": self.last_run_at,
            "last_task_id": self.last_task_id,
            "run_count": self.run_count,
            "error_count": self.error_count,
            "args": to_jsonable(self.args),
            "kwargs": to_jsonable(self.kwargs),
            "tags": dict(self.tags),
        }


class Scheduler:
    """Loop assíncrono que enfileira tasks conforme expressões cron."""

    def __init__(
        self,
        broker: Broker,
        config: Config | None = None,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_sleep: float = DEFAULT_MAX_SLEEP,
    ) -> None:
        """Cria o agendador.

        Args:
            broker: Broker usado para enfileirar as tasks disparadas.
            config: Configuração (padrão: a do broker).
            clock: Função que devolve o instante atual (epoch).
            sleep: Corrotina de espera, injetável em testes.
            max_sleep: Duração máxima de cada fatia de sono.
        """
        self.broker = broker
        self.config = config if config is not None else broker.config
        self._clock = clock
        self._sleep = sleep
        self.max_sleep = max_sleep
        self._schedules: dict[str, Schedule] = {}
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    # ------------------------------------------------------------------ configuração

    def add(self, schedule: Schedule) -> Schedule:
        """Registra um schedule.

        Raises:
            SchedulerError: Se já existir um schedule com o mesmo nome.
        """
        if schedule.name in self._schedules:
            raise SchedulerError(f"schedule {schedule.name!r} já existe")
        self._schedules[schedule.name] = schedule
        LOGGER.info(
            "schedule registrado",
            extra={"schedule": schedule.name, "cron": schedule.expression},
        )
        return schedule

    def add_task(
        self,
        task: RegisteredTask | str,
        cron: str | CronExpression,
        *args: Any,
        queue: str | None = None,
        priority: int | None = None,
        max_retries: int | None = None,
        kwargs: Mapping[str, Any] | None = None,
    ) -> Schedule:
        """Cria e registra um schedule disparado por expressão cron.

        Args:
            task: :class:`RegisteredTask` ou nome lógico da task.
            cron: Expressão cron (texto ou objeto já validado).
            *args: Argumentos fixos passados em cada disparo.
            queue: Fila (padrão: a da task).
            priority: Prioridade (padrão: a da task).
            max_retries: Retries (padrão: o da task).
            kwargs: Argumentos nomeados fixos.

        Returns:
            O :class:`Schedule` criado.

        Raises:
            CronError: Se a expressão for inválida.
            UnknownTaskError: Se o nome não estiver registrado.
        """
        expression = cron if isinstance(cron, CronExpression) else CronExpression.parse(cron)
        nome, fila, prioridade, retries = self._resolve_task(task, queue, priority, max_retries)
        schedule = Schedule(
            name=nome,
            cron=expression,
            args=list(args),
            kwargs=dict(kwargs or {}),
            queue=fila,
            priority=prioridade,
            max_retries=retries,
            tags={"scheduled": expression.expression},
        )
        return self.add(schedule)

    def add_interval(
        self,
        task: RegisteredTask | str,
        seconds: float,
        *args: Any,
        queue: str | None = None,
        priority: int | None = None,
        max_retries: int | None = None,
        kwargs: Mapping[str, Any] | None = None,
    ) -> Schedule:
        """Cria e registra um schedule disparado a cada *seconds*.

        Resolve a limitação do cron, cuja granularidade mínima é de 1 minuto:
        aqui é possível disparar a cada 5, 30 ou 90 segundos.

        Args:
            task: :class:`RegisteredTask` ou nome lógico da task.
            seconds: Intervalo entre disparos, em segundos (tem de ser > 0).
            *args: Argumentos fixos passados em cada disparo.
            queue: Fila (padrão: a da task).
            priority: Prioridade (padrão: a da task).
            max_retries: Retries (padrão: o da task).
            kwargs: Argumentos nomeados fixos.

        Returns:
            O :class:`Schedule` criado.

        Raises:
            SchedulerError: Se o intervalo for <= 0.
            UnknownTaskError: Se o nome não estiver registrado.
        """
        nome, fila, prioridade, retries = self._resolve_task(task, queue, priority, max_retries)
        schedule = Schedule(
            name=nome,
            interval=float(seconds),
            args=list(args),
            kwargs=dict(kwargs or {}),
            queue=fila,
            priority=prioridade,
            max_retries=retries,
            tags={"scheduled": f"every {format_seconds(seconds)}s"},
        )
        return self.add(schedule)

    def _resolve_task(
        self,
        task: RegisteredTask | str,
        queue: str | None,
        priority: int | None,
        max_retries: int | None,
    ) -> tuple[str, str, int, int]:
        """Normaliza task/fila/prioridade/retries a partir da task registrada."""
        if isinstance(task, RegisteredTask):
            registered = task
        else:
            registered = self.broker.registry.get(task)
        return (
            registered.name,
            queue or registered.queue,
            registered.priority if priority is None else priority,
            registered.max_retries if max_retries is None else max_retries,
        )

    def remove(self, name: str) -> bool:
        """Remove um schedule pelo nome da task."""
        return self._schedules.pop(name, None) is not None

    def schedules(self) -> list[Schedule]:
        """Lista os schedules registrados."""
        return list(self._schedules.values())

    def get(self, name: str) -> Schedule | None:
        """Devolve um schedule pelo nome, ou ``None``."""
        return self._schedules.get(name)

    # ------------------------------------------------------------------ consulta

    def compute_next(self, schedule: Schedule, now: float | None = None) -> float:
        """Calcula o instante (epoch) da próxima execução de um schedule."""
        moment = self._clock() if now is None else now
        return schedule.compute_next(moment)

    def due(self, now: float | None = None) -> list[Schedule]:
        """Devolve os schedules cujo horário já chegou (calculando o próximo se preciso)."""
        moment = self._clock() if now is None else now
        due: list[Schedule] = []
        for schedule in self._schedules.values():
            if schedule.next_run_at is None:
                schedule.next_run_at = self.compute_next(schedule, moment)
                continue
            if schedule.next_run_at <= moment:
                due.append(schedule)
        return due

    def next_runs(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Lista os próximos disparos, do mais próximo para o mais distante."""
        moment = self._clock()
        items: list[tuple[float, dict[str, Any]]] = []
        for schedule in self._schedules.values():
            when = schedule.next_run_at
            if when is None:
                try:
                    when = self.compute_next(schedule, moment)
                except CronError:
                    when = None
            if when is None:
                continue
            items.append((when, {**schedule.to_dict(), "next_run_at": when}))
        items.sort(key=lambda item: item[0])
        if limit and limit > 0:
            items = items[:limit]
        return [item[1] for item in items]

    # ------------------------------------------------------------------ execução

    async def tick(self, now: float | None = None) -> int:
        """Dispara todos os schedules vencidos e recalcula os próximos horários.

        Returns:
            Quantas tasks foram enfileiradas neste tick.
        """
        moment = self._clock() if now is None else now
        dispatched = 0
        for schedule in self.due(moment):
            try:
                task = await self.broker.submit(
                    schedule.name,
                    *schedule.args,
                    **schedule.kwargs,
                    queue=schedule.queue,
                    priority=schedule.priority,
                    max_retries=schedule.max_retries,
                    tags=schedule.tags,
                )
            except Exception as exc:
                schedule.error_count += 1
                LOGGER.error(
                    "falha ao enfileirar schedule",
                    extra={"schedule": schedule.name, "erro": f"{type(exc).__name__}: {exc}"},
                )
                try:
                    schedule.next_run_at = self.compute_next(schedule, moment)
                except CronError as cron_exc:
                    LOGGER.error(
                        "expressão cron inválida no schedule",
                        extra={"schedule": schedule.name, "erro": str(cron_exc)},
                    )
                    schedule.next_run_at = None
                continue
            schedule.last_run_at = moment
            schedule.last_task_id = task.id
            schedule.run_count += 1
            schedule.next_run_at = self.compute_next(schedule, moment)
            dispatched += 1
            LOGGER.info(
                "schedule disparado",
                extra={
                    "schedule": schedule.name,
                    "task_id": task.short_id,
                    "cron": schedule.expression,
                    "disparos": schedule.run_count,
                },
            )
        return dispatched

    async def run_forever(self) -> None:
        """Loop do agendador, em fatias de ``max_sleep`` até receber :meth:`stop`."""
        LOGGER.info("agendador iniciado", extra={"schedules": len(self._schedules)})
        try:
            while not self._stopping:
                await self._sleep(self._slice())
                await self.tick()
        finally:
            LOGGER.info("agendador encerrado", extra={"schedules": len(self._schedules)})

    def _slice(self) -> float:
        """Duração da próxima fatia de sono (nunca maior que ``max_sleep``)."""
        moment = self._clock()
        waiting = [
            schedule.next_run_at - moment
            for schedule in self._schedules.values()
            if schedule.next_run_at is not None
        ]
        if not waiting:
            return self.max_sleep
        return max(0.0, min(min(waiting), self.max_sleep))

    async def start(self) -> None:
        """Sobe o loop do agendador em background."""
        if self._task is not None:
            return
        self._stopping = False
        for schedule in self._schedules.values():
            if schedule.next_run_at is None:
                with contextlib.suppress(CronError):
                    schedule.next_run_at = self.compute_next(schedule)
        self._task = asyncio.create_task(self.run_forever(), name="taskflow:scheduler")

    async def stop(self) -> None:
        """Pede o encerramento e aguarda o loop."""
        self._stopping = True
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    @property
    def running(self) -> bool:
        """``True`` se o loop do agendador está ativo."""
        return self._task is not None and not self._task.done()


def load_schedules(
    scheduler: Scheduler,
    definitions: Sequence[Mapping[str, Any]],
) -> list[Schedule]:
    """Registra vários schedules a partir de dicionários de definição.

    Cada dicionário aceita as chaves ``name``, ``cron`` **ou** ``interval``,
    ``args``, ``kwargs``, ``queue``, ``priority`` e ``max_retries``.

    Args:
        scheduler: Agendador destino.
        definitions: Sequência de definições.

    Returns:
        Os schedules criados.

    Raises:
        KeyError: Se faltar ``name`` em alguma definição.
        CronError: Se alguma expressão cron for inválida.
        SchedulerError: Se o intervalo for <= 0.
    """
    created: list[Schedule] = []
    for definition in definitions:
            try:
                name = definition["name"]
                cron = definition.get("cron")
                interval = definition.get("interval")
            except KeyError as exc:
                raise KeyError(f"definição de schedule sem {exc.args[0]!r}: {definition!r}") from exc
            if interval is not None:
                schedule = Schedule(
                    name=str(name),
                    interval=float(interval),
                    args=list(definition.get("args") or []),
                    kwargs=dict(definition.get("kwargs") or {}),
                    queue=definition.get("queue"),
                    priority=definition.get("priority"),
                    max_retries=definition.get("max_retries"),
                    tags={"scheduled": f"every {format_seconds(interval)}s"},
                )
            else:
                expression = cron if isinstance(cron, CronExpression) else CronExpression.parse(str(cron))
                schedule = Schedule(
                    name=str(name),
                    cron=expression,
                    args=list(definition.get("args") or []),
                    kwargs=dict(definition.get("kwargs") or {}),
                    queue=definition.get("queue"),
                    priority=definition.get("priority"),
                    max_retries=definition.get("max_retries"),
                    tags={"scheduled": expression.expression},
                )
            created.append(scheduler.add(schedule))
    return created