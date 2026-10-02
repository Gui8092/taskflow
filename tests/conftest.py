"""Fixtures compartilhadas pelos testes do taskflow.

Cada teste recebe um diretório de estado temporário, um registro de tasks
isolado, um broker já iniciado e um gravador de eventos. Nenhuma fixture usa
``sleep`` longo: tempos de lease, backoff e reclaim são todos curtos.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, AsyncIterator, Iterator

import pytest

from taskflow.core.broker import Broker
from taskflow.core.config import Config, set_config
from taskflow.core.events import Event, EventBus, EventType
from taskflow.core.registry import TaskRegistry
from taskflow.core.registry import registry as global_registry
from taskflow.worker.retry import RetryPolicy


@pytest.fixture(autouse=True)
def isolated_environment() -> Iterator[None]:
    """Limpa o registro global e a configuração em cache antes e depois de cada teste."""
    global_registry.clear()
    set_config(None)
    yield
    global_registry.clear()
    set_config(None)


class EventRecorder:
    """Assina o event bus e guarda os eventos para asserções de ordem."""

    def __init__(self, bus: EventBus) -> None:
        """Liga o gravador ao bus informado."""
        self.bus = bus
        self.events: list[Event] = []
        self._subscription = bus.subscribe(self._record, name="testes")

    def _record(self, event: Event) -> None:
        """Callback síncrono chamado na publicação."""
        self.events.append(event)

    @property
    def types(self) -> list[str]:
        """Tipos dos eventos recebidos, em ordem."""
        return [event.type.value for event in self.events]

    def types_for(self, task_id: str) -> list[str]:
        """Tipos dos eventos de uma task específica, em ordem."""
        return [event.type.value for event in self.events if event.task_id == task_id]

    def of(self, event_type: EventType) -> list[Event]:
        """Eventos de um tipo específico."""
        return [event for event in self.events if event.type is event_type]

    def clear(self) -> None:
        """Descarta o que já foi gravado."""
        self.events.clear()

    async def wait_for(
        self, event_type: EventType, *, timeout: float = 2.0, task_id: str | None = None
    ) -> Event | None:
        """Aguarda um evento do tipo (e opcionalmente da task) informado."""
        return await self.bus.wait_for(
            lambda event: event.type is event_type and (task_id is None or event.task_id == task_id),
            timeout=timeout,
        )


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    """Diretório de estado isolado por teste."""
    target = tmp_path / "estado"
    target.mkdir()
    return target


@pytest.fixture
def config(data_dir: Path) -> Config:
    """Configuração com tempos curtos, adequada a testes síncronos."""
    return Config(
        data_dir=data_dir,
        queues=("default", "alta", "demo"),
        default_queue="default",
        worker_concurrency=2,
        retry_base=0.01,
        retry_cap=0.05,
        lease_seconds=1.0,
        reclaim_interval=0.05,
        compact_lines=0,
        lock_enabled=False,
    )


@pytest.fixture
def calls() -> dict[str, Any]:
    """Dicionário compartilhado onde as tasks de teste registram suas execuções."""
    return {}


@pytest.fixture
def registry(calls: dict[str, Any]) -> TaskRegistry:
    """Registro isolado com as tasks usadas pelos testes."""

    def record(name: str, value: Any = None) -> None:
        """Registra uma execução da task de teste."""
        calls.setdefault(name, []).append(value)

    local = TaskRegistry()

    @local.task(name="ok", queue="default")
    def ok(value: str = "mundo") -> str:
        """Task síncrona trivial."""
        record("ok", value)
        return f"concluído:{value}"

    @local.task(name="echo", queue="default")
    async def echo(*args: Any, **kwargs: Any) -> dict[str, Any]:
        """Task assíncrona que devolve o que recebeu."""
        record("echo", (args, kwargs))
        return {"args": list(args), "kwargs": kwargs}

    @local.task(name="flaky", queue="demo", max_retries=5)
    def flaky(fail_times: int = 2) -> str:
        """Falha ``fail_times`` vezes e depois acerta."""
        record("flaky", None)
        if len(calls["flaky"]) <= fail_times:
            raise RuntimeError("ainda não é a vez")
        return "acertei"

    @local.task(name="boom", queue="demo", max_retries=1)
    def boom() -> None:
        """Sempre falha."""
        record("boom", None)
        raise RuntimeError("falha definitiva")

    @local.task(name="lenta", queue="demo", timeout=0.2)
    async def lenta(seconds: float = 1.0) -> str:
        """Dorme ``seconds`` e devolve uma string."""
        record("lenta", seconds)
        await asyncio.sleep(seconds)
        return "demorei"

    @local.task(name="indecoravel", queue="default")
    def indecoravel() -> Any:
        """Devolve um objeto que não pode ser serializado em JSON."""
        record("indecoravel", None)
        return object()

    @local.task(name="contador", queue="default")
    def contador(index: int) -> int:
        """Devolve o índice recebido (usado para verificar duplicidade)."""
        record("contador", index)
        return index

    @local.task(name="alta", queue="alta")
    def alta(index: int) -> int:
        """Task da fila 'alta', usada nos testes de prioridade."""
        record("alta", index)
        return index

    return local


@pytest.fixture
async def broker(config: Config, registry: TaskRegistry) -> AsyncIterator[Broker]:
    """Broker com escrita, iniciado e limpo ao final do teste."""
    instance = Broker(config, registry)
    await instance.start()
    try:
        yield instance
    finally:
        await instance.stop()


@pytest.fixture
def recorder(broker: Broker) -> EventRecorder:
    """Gravador de eventos ligado ao bus do broker."""
    return EventRecorder(broker.events)


@pytest.fixture
def fast_policy() -> RetryPolicy:
    """Política de retry com backoff curtíssimo e sem jitter (determinismo)."""
    return RetryPolicy(base=0.01, cap=0.05, jitter=False)